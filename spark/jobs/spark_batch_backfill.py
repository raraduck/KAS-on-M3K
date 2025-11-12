from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/spark-data/.env")

# -------------------- 로거 설정 -------------------- #
def setup_logger():
    # log_dir = "/opt/spark-data/logs"   # ✅ 절대경로 지정
    log_dir = "/tmp/spark-logs"   # ✅ 항상 쓰기 가능한 디렉토리
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger("spark_batch_job")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.flush = sys.stdout.flush  # 즉시 출력

    # 파일 핸들러
    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"🧾 Logging started: {log_path}")
    return logger


# -------------------- 인자 파서 -------------------- #

def parse_args():
    parser = argparse.ArgumentParser(description="Spark batch job for Kafka → PostgreSQL backfill")

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--pg-port", default=os.getenv("PG_PORT", "5432"), help="PostgreSQL port")
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"), help="PostgreSQL database name")
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"), help="PostgreSQL username")
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"), help="PostgreSQL password")
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_data_lake"), help="Target PostgreSQL table name")

    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092"), help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "server-machine-usage"), help="Kafka topic to read from")
    parser.add_argument("--days", type=int, default=int(os.getenv("BACKFILL_DAYS", "1")), help="Number of days to backfill (default: 1)")

    return parser.parse_args()


# -------------------- 메인 실행 -------------------- #
def main():
    logger = setup_logger()
    args = parse_args()

    yesterday_iso = (datetime.now() - timedelta(days=args.days)).isoformat(timespec="microseconds")

    logger.info(f"📅 Backfill 기준일: {yesterday_iso}")
    logger.info(f"🎯 Kafka topic: {args.topic}")
    logger.info(f"💾 PostgreSQL: {args.pg_host}:{args.pg_port}/{args.pg_db} → {args.pg_table}")

    # Kafka JSON 스키마 정의
    schema = StructType([
        StructField("send_timestamp", StringType(), True),
        StructField("machine", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("usage", StringType(), True),
        StructField("label", DoubleType(), True),
        *[StructField(f"col_{i}", DoubleType(), True) for i in range(38)],
    ])

    spark = (
        SparkSession.builder
        .appName("SparkBatchBackfill")
        .config("spark.sql.session.timeZone", "Asia/Seoul")  # ✅ 한국 시간 명시
        .getOrCreate()
    )

    logger.info("🚀 Kafka 데이터 읽기 시작")
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    if df.rdd.isEmpty():
        logger.warning("⚠️ Kafka 토픽에서 읽을 데이터가 없습니다. 종료합니다.")
        spark.stop()
        sys.exit(0)

    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    filtered_df = json_df.filter(
        to_timestamp(col("send_timestamp")) >= to_timestamp(lit(yesterday_iso))
    )

    # 🔧 PostgreSQL 저장 전에 타입 캐스팅 추가
    filtered_df = filtered_df.withColumn("send_timestamp", to_timestamp(col("send_timestamp")))

    logger.info("📊 데이터 예시 (최대 10개)")
    filtered_df.show(10, truncate=False)

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"

    # ✅ 저장 전 행 수 카운트 및 시간 측정
    total_inserted = filtered_df.count()
    logger.info(f"🧮 저장 예정 행 수: {total_inserted}")

    start_time = datetime.now()

    try:
        logger.info("🚀 PostgreSQL 저장 시작")

        # -------------------------------
        # Spark → PostgreSQL 저장 실행
        # -------------------------------
        filtered_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", args.pg_table) \
            .option("user", args.pg_user) \
            .option("password", args.pg_pass) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        # -------------------------------
        # 저장 완료 후 로깅
        # -------------------------------
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ PostgreSQL 저장 완료: {total_inserted} rows / {elapsed:.2f}초 / 평균 {total_inserted/elapsed:.1f} row/sec")
        logger.info(f"✅ PostgreSQL 저장 테이블명: {args.pg_table}")

    except Exception as e:
        msg = str(e)
        # -------------------------------
        # 주요 예외 분기
        # -------------------------------
        if "does not exist" in msg or "UndefinedTable" in msg:
            logger.error(f"❌ PostgreSQL 테이블 '{args.pg_table}'이 존재하지 않습니다. 먼저 생성하세요.")
        elif "Connection refused" in msg or "Communications link failure" in msg:
            logger.error("❌ PostgreSQL 연결 실패 — DB 접속 정보를 확인하세요.")
        elif "password authentication failed" in msg:
            logger.error("❌ PostgreSQL 비밀번호 인증 실패")
        else:
            logger.exception(f"💥 예상치 못한 예외 발생: {msg}")
            raise

    finally:
        # -------------------------------
        # Spark 세션 종료 (항상 실행)
        # -------------------------------
        try:
            spark.stop()
            logger.info("🏁 Spark 세션 종료 완료")
        except Exception as e:
            logger.warning(f"⚠️ Spark 세션 종료 중 예외 발생: {e}")


if __name__ == "__main__":
    main()
