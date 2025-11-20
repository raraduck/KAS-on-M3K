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

# ============================================================
# Logger
# ============================================================
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


# ============================================================
# PostgreSQL UPSERT (사용자 정의)
# ============================================================
def upsert_partition(rows, args):
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

    cols = ["send_timestamp", "machine", "timestamp", "usage", "label"] + [f"col_{i}" for i in range(38)]
    placeholders = ",".join(["%s"] * len(cols))

    upsert_sql = f"""
    INSERT INTO {args.pg_table} ({",".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT (machine, timestamp, usage)
    DO NOTHING;
    """

    batch = []
    BATCH_SIZE = 500

    for row in rows:
        record = [
            row.send_timestamp,
            row.machine,
            row.timestamp,
            row.usage,
            row.label
        ]
        for i in range(38):
            record.append(getattr(row, f"col_{i}"))
        batch.append(tuple(record))

        if len(batch) >= BATCH_SIZE:
            execute_batch(cur, upsert_sql, batch)
            conn.commit()
            batch.clear()

    if batch:
        execute_batch(cur, upsert_sql, batch)
        conn.commit()

    cur.close()
    conn.close()


# ============================================================
# Args
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Spark batch job for Kafka → PostgreSQL upsert backfill")

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_data_lake"))

    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC"))

    parser.add_argument("--days", type=int, default=1)

    return parser.parse_args()


# ============================================================
# Main
# ============================================================
def main():
    logger = setup_logger()
    args = parse_args()

    day_offset_iso = (datetime.now() - timedelta(days=args.days)).isoformat(timespec="microseconds")
    logger.info(f"Backfill 기준일: {day_offset_iso}")

    # Kafka JSON schema
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
        .appName("SparkBatchBackfillUpsert")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    # 🔇 Spark 내부 INFO 로그 제거
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if df.rdd.isEmpty():
        logger.warning("Kafka 토픽 비어있음 — 종료")
        spark.stop()
        return

    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), schema).alias("data")) \
                .select("data.*")

    # 대상 기간 필터
    filtered_df = json_df.filter(
        to_timestamp(col("send_timestamp")) >= to_timestamp(lit(day_offset_iso))
    )

    # cast timestamp
    final_df = filtered_df.withColumn(
        "send_timestamp",
        to_timestamp(col("send_timestamp"))
    )

    logger.info("데이터 샘플")
    final_df.show(10, truncate=False)

    logger.info("PostgreSQL UPSERT 시작")
    # ✅ 저장 전 행 수 카운트 및 시간 측정
    total_inserted = final_df.count()
    logger.info(f"🧮 저장 예정 행 수: {total_inserted}")
    logger.info(f"데이터프레임의 파티션 수 (병렬성) 확인: {final_df.rdd.getNumPartitions()}")

    start = datetime.now()

    final_df.foreachPartition(lambda rows: upsert_partition(rows, args))

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"UPSERT 완료 — 소요시간 {elapsed:.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()
