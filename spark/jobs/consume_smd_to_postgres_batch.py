from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import os
import sys

# Kafka JSON 스키마 정의
# schema = StructType() \
#     .add("user_id", IntegerType()) \
#     .add("event", StringType()) \
#     .add("timestamp", StringType())

schema = StructType([
    StructField("timestamp", DoubleType(), True),
    *[StructField(f"col_{i}", DoubleType(), True) for i in range(38)],
    StructField("label", DoubleType(), True),
    StructField("send_timestamp", StringType(), True)
])

# Spark 세션
spark = SparkSession.builder \
    .appName("SMDToPostgresBatch") \
    .getOrCreate()

# Kafka에서 데이터 배치로 읽기 (처음부터 전체)
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka.kafka.svc.cluster.local:9092") \
    .option("subscribe", "server-machine-usage") \
    .option("startingOffsets", "earliest") \
    .load()


if df.count() == 0:
    print("Kafka 토픽에서 데이터를 찾을 수 없습니다. 종료합니다.")
    spark.stop()
    sys.exit(1)

# Kafka 메시지(JSON)를 파싱
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

print("\n[데이터 예시 출력]")
json_df.show(10, truncate=False)

# PostgreSQL에 저장 (기존 내용 덮어쓰기)
json_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://airflow-postgresql.airflow.svc.cluster.local:5432/postgres") \
    .option("dbtable", "smd_raw_data_lake") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()