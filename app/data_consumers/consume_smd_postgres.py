#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime

# -------------------- PostgreSQL 연결 정보 -------------------- #
PG_CONFIG = {
    "host": "airflow-postgresql.airflow.svc.cluster.local",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres"
}
TABLE_NAME = "smd_raw_data_lake"

# -------------------- JSON 역직렬화 -------------------- #
def json_deserializer(data):
    """Kafka 메시지를 JSON으로 역직렬화"""
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"⚠️ JSON 디코딩 오류: {e}")
        return None

# -------------------- DB 저장 함수 -------------------- #
def save_to_postgres(df):
    """pandas DataFrame을 PostgreSQL에 overwrite 저장"""
    if df.empty:
        print("⚠️ 저장할 데이터가 없습니다. 건너뜁니다.")
        return

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    # 동적 테이블 생성 (없을 시)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        timestamp TEXT,
        label INT,
        {','.join([f'col_{i} FLOAT' for i in range(38)])},
        send_timestamp TIMESTAMPTZ
    );
    """
    cur.execute(create_sql)

    # 기존 테이블 덮어쓰기(Overwrite)
    cur.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    # 컬럼명 구성
    cols = [f"col_{i}" for i in range(38)]
    col_names = ["timestamp", "label"] + cols + ["send_timestamp"]
    placeholders = ", ".join(["%s"] * len(col_names))

    # DataFrame → list of tuples
    records = []
    for _, row in df.iterrows():
        record = [row.get("timestamp"), row.get("label")]
        record += [row.get(c) for c in cols]
        record.append(row.get("send_timestamp"))
        records.append(tuple(record))

    # Batch insert (성능 개선)
    execute_batch(
        cur,
        f"INSERT INTO {TABLE_NAME} ({', '.join(col_names)}) VALUES ({placeholders})",
        records
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {len(df)}건을 PostgreSQL '{TABLE_NAME}' 테이블에 overwrite 저장 완료")

# -------------------- 메시지 처리 -------------------- #
def process_message(message):
    """Kafka 메시지를 Python dict로 변환"""
    try:
        timestamp = message.get("timestamp")
        label = int(message.get("label", 0))
        cols = {k: v for k, v in message.items() if k.startswith("col_")}
        send_ts = message.get("send_timestamp")

        # send_timestamp는 문자열 형태로 올 경우 그대로 DB가 처리 가능
        return {"timestamp": timestamp, "label": label, **cols, "send_timestamp": send_ts}

    except Exception as e:
        print(f"⚠️ 메시지 파싱 오류: {e}\n원본: {message}")
        return None

# -------------------- 메인 -------------------- #
def main():
    consumer = KafkaConsumer(
        "server-machine-usage",
        bootstrap_servers="kafka.kafka.svc.cluster.local:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="smd-consumer-group",
        value_deserializer=json_deserializer,
        consumer_timeout_ms=0  # 실시간 스트리밍 모드 (무한 대기)
    )

    print("🚀 Kafka → PostgreSQL Consumer 시작.")
    buffer = []
    batch_size = 100

    try:
        for message in consumer:
            data = process_message(message.value)
            if data:
                buffer.append(data)

            # 100건 단위로 DB 저장
            if len(buffer) >= batch_size:
                df = pd.DataFrame(buffer)
                save_to_postgres(df)
                buffer.clear()

    except KeyboardInterrupt:
        print("🛑 컨슈머 수동 종료 요청")
    finally:
        # 잔여 버퍼 처리
        if buffer:
            df = pd.DataFrame(buffer)
            save_to_postgres(df)
        consumer.close()
        print("✅ Kafka Consumer 종료 완료")


if __name__ == "__main__":
    main()
