#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import os
import sys
import argparse
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/spark-data/.env")

# -----------------------------------------------------
# Global Connection Pool (Executor당 1개씩 생성됨)
# -----------------------------------------------------
_connection_pool = None

def get_connection_pool(args):
    """
    Connection Pool 싱글톤 패턴
    각 Executor JVM에서 최초 1회만 생성
    """
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,  # Executor당 최대 5개 연결
            host=args.pg_host,
            port=args.pg_port,
            dbname=args.pg_db,
            user=args.pg_user,
            password=args.pg_pass,
            # 연결 최적화 옵션
            connect_timeout=10,
            options='-c statement_timeout=30000'  # 30초 타임아웃
        )
    return _connection_pool


# -----------------------------------------------------
# Logger
# -----------------------------------------------------
def setup_logger():
    log_dir = "/tmp/spark-logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger("spark_batch_job")
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
# 테이블 생성 함수 (최초 1회만 실행)
# -----------------------------------------------------
def ensure_table_exists(args):
    """Driver에서 테이블 생성 (1회만)"""
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_pass
    )
    cur = conn.cursor()

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {args.pg_table} (
        send_timestamp TIMESTAMPTZ,
        machine TEXT,
        timestamp TEXT,
        usage TEXT,
        label INT,
        {','.join([f'col_{i} FLOAT' for i in range(38)])},
        PRIMARY KEY (machine, timestamp, usage)
    );
    """
    
    # 인덱스 생성 (조회 성능 향상)
    index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{args.pg_table}_send_timestamp 
    ON {args.pg_table}(send_timestamp DESC);
    """
    
    cur.execute(create_sql)
    cur.execute(index_sql)
    conn.commit()
    cur.close()
    conn.close()


# -----------------------------------------------------
# Partition별 PostgreSQL 저장 (병렬 처리)
# -----------------------------------------------------
def write_partition_to_postgres(partition_iter, args):
    """
    각 Executor에서 실행되는 함수
    Connection Pool에서 연결을 가져와 병렬로 저장
    """
    import logging
    
    # Executor 로깅 설정
    logger = logging.getLogger("executor")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [EXECUTOR] %(message)s"))
        logger.addHandler(handler)
    
    conn = None
    try:
        # Connection Pool에서 연결 가져오기
        pool = get_connection_pool(args)
        conn = pool.getconn()
        cur = conn.cursor()
        
        # 컬럼 이름
        cols = [
            "send_timestamp", "machine", "timestamp", "usage", "label",
            *[f"col_{i}" for i in range(38)]
        ]
        placeholders = ",".join(["%s"] * len(cols))
        
        upsert_sql = f"""
        INSERT INTO {args.pg_table} ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (machine, timestamp, usage)
        DO NOTHING;
        """
        
        batch_records = []
        BATCH_SIZE = 500
        total_rows = 0
        
        for row in partition_iter:
            record = [
                row.send_timestamp,
                row.machine,
                row.timestamp,
                row.usage,
                row.label
            ]
            for i in range(38):
                record.append(getattr(row, f"col_{i}"))
            
            batch_records.append(tuple(record))
            
            # 배치 단위로 커밋
            if len(batch_records) >= BATCH_SIZE:
                execute_batch(cur, upsert_sql, batch_records, page_size=BATCH_SIZE)
                conn.commit()
                total_rows += len(batch_records)
                batch_records.clear()
        
        # 남은 레코드 처리
        if batch_records:
            execute_batch(cur, upsert_sql, batch_records, page_size=len(batch_records))
            conn.commit()
            total_rows += len(batch_records)
        
        cur.close()
        logger.info(f"Partition 저장 완료: {total_rows} rows")
        
    except Exception as e:
        logger.error(f"Partition 저장 실패: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Connection Pool에 연결 반환
        if conn:
            pool = get_connection_pool(args)
            pool.putconn(conn)


# -----------------------------------------------------
# foreachBatch 핸들러
# -----------------------------------------------------
def process_batch(batch_df, batch_id, args, logger):
    """
    각 마이크로배치마다 실행
    Kafka partition을 그대로 사용하여 최적 성능 달성
    """
    row_count = batch_df.count()
    if row_count == 0:
        logger.info(f"[Batch {batch_id}] 데이터 없음 → Skip")
        return
    
    num_partitions = batch_df.rdd.getNumPartitions()
    logger.info(f"[Batch {batch_id}] 시작 - Partitions: {num_partitions}, Rows: {row_count}")
    
    # Kafka partition 그대로 사용 (shuffle 없음, 최고 성능)
    batch_df.foreachPartition(lambda partition: write_partition_to_postgres(partition, args))
    
    logger.info(f"[Batch {batch_id}] 처리 완료")


# -----------------------------------------------------
# 인자 파서
# -----------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Spark Streaming with PostgreSQL Connection Pool")

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_data_lake"))

    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "server-machine-usage"))

    parser.add_argument("--checkpoint-location", default="/tmp/spark-checkpoint")
    parser.add_argument("--trigger-interval", default="10 seconds", help="마이크로배치 간격")

    return parser.parse_args()


# -----------------------------------------------------
# Main
# -----------------------------------------------------
def main():
    logger = setup_logger()
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Spark Streaming with Connection Pool 시작")
    logger.info(f"Kafka: {args.kafka_bootstrap}")
    logger.info(f"Topic: {args.topic}")
    logger.info(f"PostgreSQL: {args.pg_host}:{args.pg_port}/{args.pg_db}")
    logger.info(f"Table: {args.pg_table}")
    logger.info("=" * 60)

    # 테이블 생성 (Driver에서 1회만)
    ensure_table_exists(args)
    logger.info("테이블 생성/확인 완료")

    # Kafka JSON 스키마
    schema = StructType([
        StructField("send_timestamp", StringType(), True),
        StructField("machine", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("usage", StringType(), True),
        StructField("label", DoubleType(), True),
        *[StructField(f"col_{i}", DoubleType(), True) for i in range(38)],
    ])

    spark = (
        SparkSession.builder
        .appName("SparkStreamingConnectionPool")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .getOrCreate()
    )
    # 🔇 Spark 내부 INFO 로그 제거
    spark.sparkContext.setLogLevel("ERROR")

    # Kafka 스트림 읽기
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("failOnDataLoss", "false") \
        .load()

    # JSON 파싱 및 타입 변환
    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    final_df = (
        json_df
        .withColumn("send_timestamp", to_timestamp(col("send_timestamp")))
        .withColumn("label", col("label").cast("integer"))
    )

    # Streaming Query 시작
    query = (
        final_df.writeStream
        .outputMode("append")
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", args.checkpoint_location)
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, args, logger))
        .start()
    )

    logger.info("Streaming 시작됨. 종료하려면 Ctrl+C를 누르세요.")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 종료됨")
        query.stop()
        
        # Connection Pool 정리
        global _connection_pool
        if _connection_pool:
            _connection_pool.closeall()
            logger.info("Connection Pool 종료")


if __name__ == "__main__":
    main()