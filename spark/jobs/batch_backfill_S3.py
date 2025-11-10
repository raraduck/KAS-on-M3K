from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import os
import sys
from datetime import datetime, timedelta


# 1️⃣ 어제 시점 (ISO 8601, microseconds 포함)
yesterday_iso = (datetime.now() - timedelta(days=1)).isoformat(timespec='microseconds')

# Kafka JSON 스키마 정의
# schema = StructType() \
#     .add("user_id", IntegerType()) \
#     .add("event", StringType()) \
#     .add("timestamp", StringType())

schema = StructType([
    StructField("send_timestamp", StringType(), True),
    StructField("machine", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("label", DoubleType(), True),
    *[StructField(f"col_{i}", DoubleType(), True) for i in range(38)],
])

# Spark 세션
spark = SparkSession.builder \
    .appName("BatchBackfillS3") \
    .getOrCreate()

# # Kafka에서 데이터 배치로 읽기 (처음부터 전체)
# df = spark.read \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka.kafka.svc.cluster.local:9092") \
#     .option("subscribe", "server-machine-usage") \
#     .option("startingOffsets", "earliest") \
#     .option("endingOffsets", "latest") \
#     .load()

# if df.isEmpty():
#     print("Kafka 토픽에서 읽을 데이터가 없습니다. 종료합니다.")
#     spark.stop()
#     sys.exit(1)

# JSON 파싱
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 6️⃣ send_timestamp → timestamp 변환 + 필터링 (1줄)
filtered_df = json_df.filter(
    to_timestamp(col("send_timestamp")) >= to_timestamp(lit(yesterday_iso))
)
# 6️⃣ send_timestamp → timestamp 변환 + 필터링 (2줄-비효율)
# filtered_df = json_df \
#     .withColumn("ts", to_timestamp(col("send_timestamp"))) \
#     .filter(col("ts") >= to_timestamp(lit(yesterday_iso)))
# filtered_df = filtered_df.drop("ts")

print("\n[데이터 예시 출력]")
filtered_df.show(10, truncate=False)

# PostgreSQL에 저장 (기존 내용 덮어쓰기)
filtered_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://airflow-postgresql.airflow.svc.cluster.local:5432/postgres") \
    .option("dbtable", "smd_raw_data_lake") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()