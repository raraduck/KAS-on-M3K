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
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/spark-data/.env")

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
# PostgreSQL Upsert 저장 함수 (foreachBatch에서 호출됨)
# -----------------------------------------------------
def upsert_to_postgres(batch_df, batch_id, args, logger):

    if batch_df.count() == 0:
        logger.info(f"[Batch {batch_id}] 데이터 없음 → Skip")
        return

    logger.info(f"[Batch {batch_id}] 저장 시작 (rows={batch_df.count()})")

    # pandas 없이 Row 객체 변환
    # rows = batch_df.collect()

    pg_config = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_db,
        "user": args.pg_user,
        "password": args.pg_pass
    }
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()

    # 테이블 생성
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {args.pg_table} (
        send_timestamp TIMESTAMPTZ,
        machine TEXT,
        timestamp TEXT,
        usage TEXT,
        PRIMARY KEY (machine, timestamp, usage),
        label INT,
        {','.join([f'col_{i} FLOAT' for i in range(38)])}
    );
    """
    cur.execute(create_sql)

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

    # ------------------------------------------------------
    # collect() 대신 → 스트리밍 방식 toLocalIterator()
    # ------------------------------------------------------
    batch_records = []
    BATCH_SIZE = 500

    for r in batch_df.toLocalIterator():  # ✔ 메모리 절약
        record = [
            r.send_timestamp,
            r.machine,
            r.timestamp,
            r.usage,
            r.label
        ]
        for i in range(38):
            record.append(getattr(r, f"col_{i}"))
        batch_records.append(tuple(record))

        if len(batch_records) >= BATCH_SIZE:
            execute_batch(cur, upsert_sql, batch_records, page_size=BATCH_SIZE)
            conn.commit()
            batch_records.clear()

    # 남은 레코드 처리
    if batch_records:
        execute_batch(cur, upsert_sql, batch_records, page_size=BATCH_SIZE)
        conn.commit()

    cur.close()
    conn.close()

    logger.info(f"[Batch {batch_id}] 저장 완료")


# -----------------------------------------------------
# 인자 파서
# -----------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Spark Streaming foreachBatch → PostgreSQL Upsert")

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_data_lake"))

    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "server-machine-usage"))

    parser.add_argument("--days", type=int, default=1)

    return parser.parse_args()


# -----------------------------------------------------
# Main
# -----------------------------------------------------
def main():
    logger = setup_logger()
    args = parse_args()

    day_offset_iso = (datetime.now() - timedelta(days=args.days)).isoformat()

    logger.info(f"Backfill 기준일: {day_offset_iso}")

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
        .appName("SparkForeachBatchRealtime")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )

    # Kafka → JSON
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .load()
        # .option("kafka.group.id", "spark-realtime-group") \ # 그룹생성이 애초에 되지 않음

    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # PK 기준 중복 제거: Spark 쪽에서 굳이 dropDuplicates로 상태를 유지할 필요가 없음
    # dedup_df = json_df.dropDuplicates(["machine", "timestamp", "usage"])

    # 타입 캐스팅
    final_df = (
        json_df
        .withColumn("send_timestamp", to_timestamp(col("send_timestamp")))
    )

    # -----------------------------------------------------
    # foreachBatch 적용 → PostgreSQL Upsert
    # -----------------------------------------------------
    query = (
        final_df.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id:
                      upsert_to_postgres(batch_df, batch_id, args, logger))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
