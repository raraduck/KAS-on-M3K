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
from pyspark.ml.linalg import Vectors, DenseVector, VectorUDT
from pyspark.sql.functions import udf, col, lit, avg

import numpy as np
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
        .load()
    )
    return df


        # 필요하면 병렬 읽기 옵션 추가 (send_timestamp를 partitionColumn으로 쓰기 애매하면 생략)
        # .option("numPartitions", 4)
        # .option("partitionColumn", "send_timestamp")
        # .option("lowerBound", ...)
        # .option("upperBound", ...)

# -----------------------------------------------------
# foreachBatch용 핸들러
# -----------------------------------------------------
def make_batch_handler(spark, args, logger,
        assembler, pca_model, components_T, mean_row, threshold, err_mean, err_std
    ):

    def _process_batch(_dummy_df, batch_id):
        logger.info("=" * 60)
        logger.info(f"[Batch {batch_id}] 시작")

        kst = pytz.timezone("Asia/Seoul")
        last_ts = get_last_checkpoint(args, logger)
        current_ts = datetime.now(tz=kst)

        # current_ts = datetime.now(tz=kst)
        # # current_ts = datetime.utcnow()
        # last_ts = get_last_checkpoint(args, logger)

        # --------------------------------------------------
        # 1) PostgreSQL에서 증분 데이터 로드
        # --------------------------------------------------
        df = load_incremental_from_postgres(spark, args, last_ts, current_ts, logger)
        row_count = df.count()
        if row_count == 0:
            logger.info(f"[Batch {batch_id}] 증분 데이터 없음 → 스킵")
            update_checkpoint(args, logger, current_ts)
            return

        logger.info(f"[Batch {batch_id}] 증분 데이터 로드: {row_count} rows")
        

        # --------------------------------------------------
        # 2) PCA 기반 anomaly detection
        # --------------------------------------------------
        feature_cols = [f"col_{i}" for i in range(38)]

        # mean centering
        for c in feature_cols:
            df = df.withColumn(c, col(c) - lit(mean_row[c]))

        # assemble
        vec_df = assembler.transform(df)

        # pca transform
        pca_df = pca_model.transform(vec_df)

        # reconstruction udf
        @udf(returnType=VectorUDT())
        def reconstruct(pca_vec):
            p = np.array(pca_vec.toArray())  # (k,)
            x_hat = p.dot(components_T)      # reconstructed (38,)
            return Vectors.dense(x_hat)

        recon_df = pca_df.withColumn("reconstructed", reconstruct(col("pca_features")))

        # reconstruction error udf
        @udf(returnType=DoubleType())
        def recon_error(x, x_hat):
            a = np.array(x.toArray())
            b = np.array(x_hat.toArray())
            return float(np.sum((a - b) ** 2))

        scored_df = recon_df.withColumn(
            "recon_error", recon_error(col("features"), col("reconstructed"))
        )
        zscore_df = scored_df.withColumn(
            "zscore",
            (F.col("recon_error") - F.lit(err_mean)) / F.lit(err_std)
        )
        # anomaly flag
        result_df = zscore_df.withColumn(
            "is_anomaly", F.col("recon_error") > lit(threshold)
        )

        anomaly_df = result_df.filter(F.col("is_anomaly") == True)
        anomaly_count = anomaly_df.count()
        logger.info(f"[Batch {batch_id}] anomaly count = {anomaly_count}")

        # timestamp를 string으로 변환
        kafka_df = anomaly_df.select(
            F.col("machine").alias("key"),
            F.to_json(
                F.struct(
                    F.col("send_timestamp").cast("string").alias("send_timestamp"),
                    F.col("machine"),
                    F.col("timestamp").cast("string").alias("timestamp"),
                    F.col("zscore")
                )
            ).alias("value")
        )

        (kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("topic", args.eph_topic)
            .save()
        )


        # # JSON 직렬화
        # kafka_df = json_df.selectExpr(
        #     "machine AS key",                     # Kafka key
        #     "to_json(struct(*)) AS value"         # 원하는 JSON
        # )

        # # Kafka write
        # (kafka_df.write
        #     .format("kafka")
        #     .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        #     .option("topic", args.eph_topic)
        #     .save()
        # )

        logger.info(f"[Batch {batch_id}] anomaly 결과 Kafka 전송 완료")

        # --------------------------------------------------
        # 4) 체크포인트 갱신
        # --------------------------------------------------
        max_ts = df.agg(F.max("send_timestamp")).collect()[0][0] or current_ts
        update_checkpoint(args, logger, max_ts)

        logger.info(f"[Batch {batch_id}] 종료")

        # # 4) 체크포인트 갱신 (이번에 처리한 send_timestamp 최대값 기준)
        # max_ts = df.agg(F.max("send_timestamp")).collect()[0][0] or current_ts
        # update_checkpoint(args, logger, max_ts)

        # logger.info(f"[Batch {batch_id}] 종료")
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

    parser.add_argument("--machine", default="machine-1-1")
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET", "s3a://kas-on-m3k/smd-dataset"))
    parser.add_argument("--format", default=os.getenv("S3_FORMAT", "csv"))

    return parser.parse_args()


def initialization(args, spark, logger):
    s3_path = f"{args.s3_bucket}/{args.machine}"

    logger.info(f"📥 Loading PCA training dataset: {s3_path}")

    # sdf_train = spark.createDataFrame(df_train)
    sdf_train = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(s3_path)
    )
    feature_cols = [c for c in sdf_train.columns if 'col' in c]
    mean_row = sdf_train.select([avg(c).alias(c) for c in feature_cols]).collect()[0]

    for c in feature_cols:
        sdf_train = sdf_train.withColumn(c, col(c) - lit(mean_row[c]))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    vec_train = assembler.transform(sdf_train).select("features")

    # pca_k = 9
    pca = PCA(k=args.pca_k, inputCol="features", outputCol="pca_features")
    pca_model = pca.fit(vec_train)

    train_pca = pca_model.transform(vec_train)

    components = np.array(pca_model.pc.toArray())
    components_T = components.T

    @udf(returnType=VectorUDT())
    def reconstruct(pca_vec):
        p = np.array(pca_vec.toArray())                 # shape (k,)
        x_hat = p.dot(components_T)                     # no mean added
        return Vectors.dense(x_hat)

    train_recon = train_pca.withColumn("reconstructed", reconstruct(col("pca_features")))

    @udf(returnType=DoubleType())
    def recon_error(x, x_hat):
        a = np.array(x.toArray())
        b = np.array(x_hat.toArray())
        return float(np.sum((a - b)**2))

    train_err = train_recon.withColumn("recon_error", recon_error(col("features"), col("reconstructed")))
    errors = [row["recon_error"] for row in train_err.select("recon_error").collect()]
    threshold = np.quantile(errors, 0.99)

    err_mean = np.mean(errors)
    err_std  = np.std(errors)
    # 🟩 반환해야 스트리밍에서 anomaly detection 가능
    return assembler, pca_model, components_T, mean_row.asDict(), threshold, err_mean, err_std



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
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") 
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        # .config("spark.sql.session.timeZone", "UTC")
        # .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    assembler, pca_model, components_T, mean_row_dict, threshold, err_mean, err_std = initialization(args, spark, logger)

    # rate 소스로 단순 트리거만 발생시키는 스트리밍
    rate_df = (spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)   # 의미 없는 더미, trigger용
        .load()
    )

    # query = (rate_df.writeStream
    #     .outputMode("update")
    #     .trigger(processingTime=args.trigger_interval)
    #     .option("checkpointLocation", args.checkpoint_location)
    #     .foreachBatch(make_batch_handler(spark, args, logger))
    #     .start()
    # )

    query = (
    rate_df.writeStream
        .outputMode("update")
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", args.checkpoint_location)
        .foreachBatch(
            make_batch_handler(
                spark, args, logger,
                assembler, pca_model, components_T, mean_row_dict, threshold, err_mean, err_std
            )
        )
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
