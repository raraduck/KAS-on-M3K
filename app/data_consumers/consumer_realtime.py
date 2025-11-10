#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime
import argparse
import os
from dotenv import load_dotenv

# .env 파일 불러오기 (기본 경로: 현재 실행 디렉토리)
load_dotenv()

# -------------------- PostgreSQL 연결 정보 -------------------- #
# PG_CONFIG = {
#     "host": "airflow-postgresql.airflow.svc.cluster.local",
#     "port": 5432,
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "postgres"
# }
# TABLE_NAME = "smd_table_realtime"

# -------------------- JSON 역직렬화 -------------------- #
def json_deserializer(data):
    """Kafka 메시지를 JSON으로 역직렬화"""
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"⚠️ JSON 디코딩 오류: {e}")
        return None

# -------------------- DB 저장 함수 -------------------- #
def save_to_postgres(df, pg_config, table_name):
    """pandas DataFrame을 PostgreSQL에 overwrite 저장"""
    if df.empty:
        print("⚠️ 저장할 데이터가 없습니다. 건너뜁니다.")
        return

    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()

    # 동적 테이블 생성 (없을 시)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        send_timestamp TIMESTAMPTZ,
        machine TEXT,
        timestamp TEXT,
        label INT,
        {','.join([f'col_{i} FLOAT' for i in range(38)])}
    );
    """
    cur.execute(create_sql)

    # # 기존 테이블 덮어쓰기(Overwrite)
    # cur.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    # 컬럼명 구성
    cols = [f"col_{i}" for i in range(38)]
    col_names = ["send_timestamp", "machine", "timestamp", "label"] + cols
    placeholders = ", ".join(["%s"] * len(col_names))

    # DataFrame → list of tuples
    records = []
    for _, row in df.iterrows():
        record = [
            row.get("send_timestamp"),
            row.get("machine"),
            row.get("timestamp"),
            row.get("label")
        ]
        record += [row.get(c) for c in cols]
        records.append(tuple(record))

    # assert len(record) == len(col_names)

    # Batch insert (성능 개선)
    execute_batch(
        cur,
        f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})",
        records
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {len(df)}건을 PostgreSQL '{table_name}' 테이블에 overwrite 저장 완료")

# -------------------- 메시지 처리 -------------------- #
def process_message(message):
    """Kafka 메시지를 Python dict로 변환"""
    try:
        send_ts = message.get("send_timestamp")
        machine = message.get("machine")
        timestamp = message.get("timestamp")
        label = int(message.get("label", 0))
        cols = {k: v for k, v in message.items() if k.startswith("col_")}

        # send_timestamp는 문자열 형태로 올 경우 그대로 DB가 처리 가능
        return {"send_timestamp": send_ts, "machine": machine, "timestamp": timestamp, "label": label, **cols}

    except Exception as e:
        print(f"⚠️ 메시지 파싱 오류: {e}\n원본: {message}")
        return None

# -------------------- 메인 -------------------- #
def main():
    parser = argparse.ArgumentParser(description="Kafka → PostgreSQL Consumer")

    # Kafka 설정
    parser.add_argument('--topic', default='test-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                     type=str, help='Kafka 부트스트랩 서버')
    parser.add_argument("--group-id", default="smd-realtime-group", help="Kafka consumer group ID")
    parser.add_argument("--timeout", type=int, default=90000, help='메시지 타임아웃 (단위 밀리초), default: 90000')

    # PostgreSQL 설정
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", 5432)))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_table_realtime"))

    parser.add_argument("--batch-size", type=int, default=100, help="Postgres로 저장할 batch 크기")

    args = parser.parse_args()

    pg_config = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_db,
        "user": args.pg_user,
        "password": args.pg_pass,
    }

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers.split(","), # "kafka.kafka.svc.cluster.local:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id, # "smd-consumer-group",
        value_deserializer=json_deserializer,
        consumer_timeout_ms=args.timeout
    )

    print("🚀 Kafka → PostgreSQL Consumer 시작.")
    buffer = []

    try:
        for message in consumer:
            data = process_message(message.value)
            if data:
                buffer.append(data)

            # 100건 단위로 DB 저장
            if len(buffer) >= args.batch_size:
                df = pd.DataFrame(buffer)
                save_to_postgres(df, pg_config, args.pg_table)
                buffer.clear()

    except KeyboardInterrupt:
        print("🛑 컨슈머 수동 종료 요청")
    finally:
        # 잔여 버퍼 처리
        if buffer:
            df = pd.DataFrame(buffer)
            save_to_postgres(df, pg_config, args.pg_table)
        consumer.close()
        print("✅ Kafka Consumer 종료 완료")


if __name__ == "__main__":
    main()
