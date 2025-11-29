#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
import pytz

from dotenv import load_dotenv
load_dotenv("/opt/spark-data/.env")

import psycopg2

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.linalg import Vectors, DenseVector

# -----------------------------------------------------
# Kafka Topic 생성
# -----------------------------------------------------
def create_ephemeral_topic(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='ad_topic_creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=1,               # AD 결과량이 많지 않다고 가정
        replication_factor=3,
        topic_configs={
            "retention.ms": "86400000",         # 1일 보관 (필요에 맞게 조정)
            "segment.bytes": "1048576",
            "cleanup.policy": "delete",
            "min.insync.replicas": "1",
        }
    )

    try:
        admin_client.create_topics([topic])
        print(f"✅ AD topic created: {topic_name}")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic already exists: {topic_name}")
    finally:
        admin_client.close()


# -----------------------------------------------------
# Logger
# -----------------------------------------------------
def setup_logger():
    log_dir = "/tmp/spark-logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"downstream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger("spark_downstream_job")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.flush = sys.stdout.flush

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)

    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info(f"Logging started: {log_path}")
    return logger


# -----------------------------------------------------
# 체크포인트 테이블 관리 (PostgreSQL 메타데이터)
# -----------------------------------------------------
def get_pg_conn(args):
    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_pass,
        connect_timeout=10,
    )

def ensure_checkpoint_table(args, logger):
    conn = get_pg_conn(args)
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {args.pg_checkpoint_table} (
        job_name TEXT PRIMARY KEY,
        last_ts TIMESTAMPTZ
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"체크포인트 테이블 확인/생성 완료: {args.pg_checkpoint_table}")

def get_last_checkpoint(args, logger):
    conn = get_pg_conn(args)
    cur = conn.cursor()
    cur.execute(f"SELECT last_ts FROM {args.pg_checkpoint_table} WHERE job_name=%s", (args.job_name,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row and row[0]:
        logger.info(f"이전 체크포인트: {row[0]}")
        return row[0]
    else:
        logger.info("이전 체크포인트 없음 → 초기값 사용")
        return None

def update_checkpoint(args, logger, new_ts):
    conn = get_pg_conn(args)
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {args.pg_checkpoint_table} (job_name, last_ts)
        VALUES (%s, %s)
        ON CONFLICT (job_name)
        DO UPDATE SET last_ts = EXCLUDED.last_ts;
        """,
        (args.job_name, new_ts)
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"체크포인트 갱신: {new_ts}")


# -----------------------------------------------------
# PostgreSQL에서 증분 데이터 로드
# -----------------------------------------------------
def load_incremental_from_postgres(spark, args, last_ts, current_ts, logger):
    """
    send_timestamp ∈ (last_ts, current_ts] 구간만 읽어오는 증분 쿼리
    """
    if last_ts is None:
        # 초기에는 lookback_minutes 만큼 과거부터 읽기
        last_ts = current_ts - timedelta(minutes=args.lookback_minutes)

    logger.info(f"증분 구간: ({last_ts}, {current_ts}]")

    query = f"""
        (SELECT *
         FROM {args.pg_table}
         WHERE send_timestamp > '{last_ts.isoformat()}'
           AND send_timestamp <= '{current_ts.isoformat()}') AS src
    """

    df = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}")
        .option("dbtable", query)
        .option("user", args.pg_user)
        .option("password", args.pg_pass)
        .option("driver", "org.postgresql.Driver")
        .option("sessionInitStatement", "SET TIMEZONE TO 'Asia/Seoul'")
        # 필요하면 병렬 읽기 옵션 추가 (send_timestamp를 partitionColumn으로 쓰기 애매하면 생략)
        # .option("numPartitions", 4)
        # .option("partitionColumn", "send_timestamp")
        # .option("lowerBound", ...)
        # .option("upperBound", ...)
        .load()
    )

    return df


# -----------------------------------------------------
# PCA Reconstruction 기반 이상감지
# -----------------------------------------------------
def run_pca_anomaly_detection(df, args, logger):
    """
    - feature: col_0 ~ col_37 만 사용
    - StandardScaler -> PCA(k) 학습 (배치 단위)
    - reconstruction error = ||x - x_hat||^2
    - 배치 내 score 분포로 z-score 계산
    - abs(zscore) >= threshold 인 것만 anomaly 로 반환
    """

    feature_cols = [f"col_{i}" for i in range(38)]
    for c in feature_cols:
        if c not in df.columns:
            raise ValueError(f"Feature column missing: {c}")

    # NaN 제거 (필요시 더 정교한 imputation 가능)
    df_clean = df.dropna(subset=feature_cols)

    if df_clean.rdd.isEmpty():
        logger.info("정제된 데이터가 없음 (NaN 제거 후) → AD 스킵")
        return None

    # 1. VectorAssembler
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    vec_df = assembler.transform(df_clean)

    # 2. StandardScaler (mean=0, var=1)
    scaler = StandardScaler(withMean=True, withStd=True, inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(vec_df)
    scaled_df = scaler_model.transform(vec_df)

    # 3. PCA (args.pca_k 차원으로 축소)
    pca = PCA(k=args.pca_k, inputCol="scaledFeatures", outputCol="pcaFeatures")
    pca_model = pca.fit(scaled_df)
    pca_df = pca_model.transform(scaled_df)

    # 4. Reconstruction: x_hat = (x * V_k) * V_k^T
    #    Spark PCA 모델의 pc는 DenseMatrix (열 = 주성분)
    pc = pca_model.pc.toArray()  # shape: [n_features, k]
    import numpy as np

    Vk = np.array(pc)  # [n_features, k]

    # BroadCast to executors
    spark = df.sparkSession
    bc_Vk = spark.sparkContext.broadcast(Vk)

    def reconstruction_error(scaled_vec):
        """
        scaled_vec: DenseVector (표준화된 x)
        return: squared L2 norm of (x - x_hat)
        """
        if scaled_vec is None:
            return float("nan")
        x = np.array(scaled_vec)
        V = bc_Vk.value  # [n_features, k]
        # low-dim representation
        z = x.dot(V)             # [k]
        # reconstruct
        x_hat = z.dot(V.T)       # [n_features]
        diff = x - x_hat
        return float(np.dot(diff, diff))   # ||x - x_hat||^2

    from pyspark.sql.functions import udf
    recon_err_udf = udf(reconstruction_error, DoubleType())

    scored_df = pca_df.withColumn("recon_error", recon_err_udf(F.col("scaledFeatures")))

    # 5. z-score 계산 (배치 전체에서 평균/표준편차)
    stats = scored_df.agg(
        F.avg("recon_error").alias("mu"),
        F.stddev("recon_error").alias("sigma")
    ).collect()[0]

    mu = stats["mu"]
    sigma = stats["sigma"] if stats["sigma"] not in (0, None) else 1e-9

    logger.info(f"recon_error 통계: mu={mu:.6f}, sigma={sigma:.6f}")

    scored_df = scored_df.withColumn(
        "zscore",
        (F.col("recon_error") - F.lit(mu)) / F.lit(sigma)
    )

    # 6. threshold 이상만 anomaly 로 필터링
    anomalies = scored_df.filter(F.abs(F.col("zscore")) >= F.lit(args.p_value))

    if anomalies.rdd.isEmpty():
        logger.info(f"이번 배치에서 anomaly 없음 (threshold={args.p_value})")
        return None

    logger.info(f"이번 배치 anomaly 개수: {anomalies.count()}")

    # Kafka로 보낼 때 필요한 필드만 남기거나, 전체를 JSON으로 쏴도 됨
    # 여기서는 주요 메타 + score만 남기고 예시
    out_cols = [
        "send_timestamp", "machine", "timestamp", "usage", "label",
        "recon_error", "zscore"
    ] + feature_cols

    return anomalies.select(*[c for c in out_cols if c in anomalies.columns])


# -----------------------------------------------------
# foreachBatch용 핸들러
# -----------------------------------------------------
def make_batch_handler(spark, args, logger):
    def _process_batch(_dummy_df, batch_id):
        logger.info("=" * 60)
        logger.info(f"[Batch {batch_id}] 시작")

        kst = pytz.timezone("Asia/Seoul")

        current_ts = datetime.now(tz=kst)
        # current_ts = datetime.utcnow()
        last_ts = get_last_checkpoint(args, logger)

        # 1) PostgreSQL에서 증분 데이터 로드
        df = load_incremental_from_postgres(spark, args, last_ts, current_ts, logger)
        row_count = df.count()
        if row_count == 0:
            logger.info(f"[Batch {batch_id}] 증분 데이터 없음 → 스킵")
            update_checkpoint(args, logger, current_ts)
            return

        logger.info(f"[Batch {batch_id}] 증분 데이터 로드: {row_count} rows")

        # # 2) 이상감지 수행
        # anomalies = run_pca_anomaly_detection(df, args, logger)
        # if anomalies is None:
        #     # anomaly 없더라도 checkpoint는 갱신
        #     max_ts = df.agg(F.max("send_timestamp")).collect()[0][0] or current_ts
        #     update_checkpoint(args, logger, max_ts)
        #     logger.info(f"[Batch {batch_id}] anomaly 없음, 종료")
        #     return

        # # 3) anomaly 결과를 Kafka topic 으로 write (배치 모드)
        # kafka_df = anomalies.withColumn(
        #     "key", F.col("machine").cast("string")
        # ).selectExpr(
        #     "CAST(key AS STRING) AS key",
        #     "to_json(struct(*)) AS value"
        # )

        # (kafka_df.write
        #     .format("kafka")
        #     .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        #     .option("topic", args.eph_topic)
        #     .save())

        # 원하는 zscore 고정값
        fixed_zscore = 1.99   # 필요하면 args에서 받아도 됨

        # struct 로 필요한 필드만 구성
        json_df = df.select(
            F.col("send_timestamp").cast("string"),
            F.col("machine"),
            F.col("timestamp").cast("string"),   # JSON 형태에 맞춰 string 변환 권장
            F.lit(fixed_zscore).alias("zscore")
        )

        # JSON 직렬화
        kafka_df = json_df.selectExpr(
            "machine AS key",                     # Kafka key
            "to_json(struct(*)) AS value"         # 원하는 JSON
        )

        # Kafka write
        (kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("topic", args.eph_topic)
            .save()
        )

        logger.info(f"[Batch {batch_id}] anomaly 결과 Kafka 전송 완료")

        # 4) 체크포인트 갱신 (이번에 처리한 send_timestamp 최대값 기준)
        max_ts = df.agg(F.max("send_timestamp")).collect()[0][0] or current_ts
        update_checkpoint(args, logger, max_ts)

        logger.info(f"[Batch {batch_id}] 종료")
    return _process_batch


# -----------------------------------------------------
# 인자 파서
# -----------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Downstream AD: PostgreSQL -> PCA -> Kafka")

    # PostgreSQL (Data Lake)
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "datalake_table"))

    # 체크포인트 메타 테이블
    parser.add_argument("--pg-checkpoint-table", dest="pg_checkpoint_table",
                        default=os.getenv("PG_CHECKPOINT_TABLE", "ad_checkpoint"))
    parser.add_argument("--job-name", default=os.getenv("AD_JOB_NAME", "ad_downstream_job"))

    # Kafka
    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092"))
    parser.add_argument("--eph-topic", default=os.getenv("KAFKA_AD_TOPIC", "anomaly-detection-topic"))

    # Streaming 설정
    parser.add_argument("--checkpoint-location", default="/tmp/spark-ad-checkpoint")
    parser.add_argument("--trigger-interval", default="60 seconds", help="마이크로배치 간격")
    parser.add_argument("--lookback-minutes", type=int, default=5,
                        help="첫 실행 시 과거 몇 분까지 읽을지")

    # AD 설정
    parser.add_argument("--pca-k", type=int, default=5, help="PCA 차원 수")
    parser.add_argument("--p-value", type=float, default=0.05,
                        help="|pvalue| >= threshold 를 anomaly 로 간주")

    return parser.parse_args()


# -----------------------------------------------------
# Main
# -----------------------------------------------------
def main():
    logger = setup_logger()
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Downstream AD Streaming 시작")
    logger.info(f"PostgreSQL: {args.pg_host}:{args.pg_port}/{args.pg_db}")
    logger.info(f"Target Table: {args.pg_table}")
    logger.info(f"Checkpoint Table: {args.pg_checkpoint_table} (job={args.job_name})")
    logger.info(f"Kafka Bootstrap: {args.kafka_bootstrap}")
    logger.info(f"AD Topic: {args.eph_topic}")
    logger.info("=" * 60)

    # AD 결과 토픽 생성
    if args.eph_topic != "None":
        logger.info(f"🟦 AD topic 생성 시도: {args.eph_topic}")
        create_ephemeral_topic(
            bootstrap_servers=args.kafka_bootstrap,
            topic_name=args.eph_topic
        )
        logger.info(f"🟩 AD topic 준비 완료: {args.eph_topic}")

    # 체크포인트 테이블 준비
    ensure_checkpoint_table(args, logger)

    spark = (
        SparkSession.builder
        .appName("DownstreamPCAAnomalyDetection")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # rate 소스로 단순 트리거만 발생시키는 스트리밍
    rate_df = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)   # 의미 없는 더미, trigger용
        .load()
    )

    query = (rate_df.writeStream
        .outputMode("update")
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", args.checkpoint_location)
        .foreachBatch(make_batch_handler(spark, args, logger))
        .start()
    )

    logger.info("Streaming Query 시작됨. 종료하려면 Ctrl+C를 누르세요.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 종료됨")
        query.stop()
        logger.info("Streaming 중지")


if __name__ == "__main__":
    main()
