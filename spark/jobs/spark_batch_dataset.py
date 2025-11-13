from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment file
load_dotenv("/opt/spark-data/.env")


# -------------------- 로거 설정 -------------------- #
def setup_logger():
    log_dir = "/tmp/spark-logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger("spark_batch_dataset")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, "w", "utf-8")
    file_handler.setFormatter(formatter)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.info(f"🧾 Logging started: {log_path}")
    return logger


# -------------------- 인자 파서 -------------------- #
def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL → S3 Spark batch job")

    # PostgreSQL
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_data_lake"))

    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET", "s3a://kas-on-m3k/smd-dataset"))
    parser.add_argument("--format", default=os.getenv("S3_FORMAT", "csv"))

    # Backfill 일수
    parser.add_argument("--days", type=int, default=int(os.getenv("BACKFILL_DAYS", "1")))

    return parser.parse_args()


# -------------------- 메인 실행 -------------------- #
def main():
    logger = setup_logger()
    args = parse_args()

    # SparkSession 초기화
    # spark = SparkSession.builder.appName("SparkBatchDataset").getOrCreate()
    spark = (
        SparkSession.builder
        .appName("SparkBatchDataset")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") 
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    logger.info(f"🔗 Connecting to PostgreSQL {jdbc_url} ...")

    cutoff = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        (SELECT *
        FROM {args.pg_table}
        WHERE send_timestamp >= '{cutoff}'
        AND usage = 'train'
        ) AS sub
    """

    # 1️⃣ PostgreSQL → DataFrame
    # df = spark.read.format("jdbc") \
    #     .option("url", jdbc_url) \
    #     .option("dbtable", args.pg_table) \
    #     .option("user", args.pg_user) \
    #     .option("password", args.pg_pass) \
    #     .option("driver", "org.postgresql.Driver") \
    #     .load()
    df = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)      # ← dbtable 대신 query 사용
            .option("user", args.pg_user)
            .option("password", args.pg_pass)
            .option("driver", "org.postgresql.Driver")
            .load()
    )

    if df.rdd.isEmpty():
        logger.warning("⚠️ No data found in PostgreSQL table. Exiting.")
        spark.stop()
        sys.exit(0)

    logger.info(f"✅ Loaded {df.count()} rows from {args.pg_table}")

    # 2️⃣ 필터링: 최근 N일 데이터만
    df = df.withColumn("send_timestamp", to_timestamp(col("send_timestamp")))
    cutoff = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d %H:%M:%S")
    filtered_df = df.filter(col("send_timestamp") >= lit(cutoff))
    count = filtered_df.count()
    logger.info(f"📅 Filtered {count} rows after {cutoff}")

    # 2️⃣ 필터링: 학습용 데이터만
    filtered_df = filtered_df.filter(col("usage") == "train")
    count = filtered_df.count()
    logger.info(f"📅 Filtered {count} rows only for train")

    # # 3️⃣ S3 저장
    s3_path = f"{args.s3_bucket}/export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"💾 Saving to S3 path: {s3_path} (format={args.format})")

    if args.format == "csv":
        filtered_df.write.mode("overwrite").option("header", True).csv(s3_path)
    else:
        filtered_df.write.mode("overwrite").parquet(s3_path)

    logger.info(f"✅ Export complete: {count} rows saved to {s3_path}")

    spark.stop()
    logger.info("🏁 Spark session closed.")


if __name__ == "__main__":
    main()
