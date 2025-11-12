#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime
import argparse
import os
from dotenv import load_dotenv
import logging

# .env 파일 불러오기 (기본 경로: 현재 실행 디렉토리)
load_dotenv()

# -------------------- 로거 전역 선언 -------------------- #
logger = None

def setup_logger():
    """로거 설정: 콘솔 + 파일 출력"""
    # 로그 디렉터리 생성
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 파일명: 실행 시각 기반
    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    # 로거 생성
    logger_obj = logging.getLogger()
    logger_obj.setLevel(logging.INFO)

    # 포맷 지정
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 스트림 핸들러 (콘솔용)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.flush = sys.stdout.flush  # ✅ 즉시 출력용

    # 파일 핸들러 (로그파일용)
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 기존 핸들러 제거 후 재등록 (중복 방지)
    if logger_obj.hasHandlers():
        logger_obj.handlers.clear()
    logger_obj.addHandler(console_handler)
    logger_obj.addHandler(file_handler)

    logging.info(f"🧾 Logging started: {log_filename}")

    return logger_obj

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
        logger.warnming(f"⚠️ JSON 디코딩 오류: {e}")
        return None

# -------------------- DB 저장 함수 -------------------- #
def save_to_postgres(df, pg_config, table_name):
    """pandas DataFrame을 PostgreSQL에 overwrite 저장"""
    if df.empty:
        logger.warnming("⚠️ 저장할 데이터가 없습니다. 건너뜁니다.")
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
        usage TEXT,
        label INT,
        {','.join([f'col_{i} FLOAT' for i in range(38)])}
    );
    """
    cur.execute(create_sql)

    # # 기존 테이블 덮어쓰기(Overwrite)
    # cur.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    # 컬럼명 구성
    cols = [f"col_{i}" for i in range(38)]
    col_names = ["send_timestamp", "machine", "timestamp", "usage", "label"] + cols
    placeholders = ", ".join(["%s"] * len(col_names))

    # DataFrame → list of tuples
    records = []
    for _, row in df.iterrows():
        record = [
            row.get("send_timestamp"),
            row.get("machine"),
            row.get("timestamp"),
            row.get("usage"),
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
    logger.info(f"💾 {len(df)}건을 PostgreSQL '{table_name}' 테이블에 overwrite 저장 완료")

# -------------------- 메시지 처리 -------------------- #
def process_message(message):
    """Kafka 메시지를 Python dict로 변환"""
    try:
        send_ts = message.get("send_timestamp")
        machine = message.get("machine")
        timestamp = message.get("timestamp")
        usage = message.get("usage")
        label = int(message.get("label", 0))
        cols = {k: v for k, v in message.items() if k.startswith("col_")}

        # send_timestamp는 문자열 형태로 올 경우 그대로 DB가 처리 가능
        return {"send_timestamp": send_ts, "machine": machine, "timestamp": timestamp, "usage": usage, "label": label, **cols}

    except Exception as e:
        logger.warnming(f"⚠️ 메시지 파싱 오류: {e}\n원본: {message}")
        return None

# -------------------- 메인 -------------------- #
def main():
    global logger
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

    logger.info("🚀 Kafka → PostgreSQL Consumer 시작.")
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
        logger.error("🛑 컨슈머 수동 종료 요청")
    finally:
        # 잔여 버퍼 처리
        if buffer:
            df = pd.DataFrame(buffer)
            save_to_postgres(df, pg_config, args.pg_table)
        consumer.close()
        logger.info("✅ Kafka Consumer 종료 완료")


if __name__ == "__main__":
    logger = setup_logger()
    main()
