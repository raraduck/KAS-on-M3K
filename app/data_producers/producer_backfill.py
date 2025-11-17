#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import glob
import csv
import json
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv   # ✅ .env 파일 지원

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


# logger 파일로 저장
# try catch 자세하게 적용

# -------------------- 토픽 생성 -------------------- #
def create_topic(bootstrap_servers, topic_name, num_partitions=3, replication_factor=3):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic_creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        topic_configs={
            "retention.ms": "300000", # 분
            "cleanup.policy": "delete"
        }
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"✅ 토픽 생성 완료: {topic_name} (partitions={num_partitions}, replicas={replication_factor})")
    except TopicAlreadyExistsError:
        logger.warning(f"⚠️ 토픽 '{topic_name}'은 이미 존재합니다.")
    finally:
        admin_client.close()


# -------------------- JSON 직렬화 -------------------- #
def json_serializer(data):
    """데이터를 JSON 형식으로 직렬화"""
    return json.dumps(data).encode('utf-8')


# -------------------- CSV 파일 제너레이터 -------------------- #
def iter_all_csv_rows(machine, base_dir):
    """
    data/machine-*-*/*_train.csv 파일을 전부 순회하며 각 row yield
    """
    target_machine = "machine-*" if machine == "all" else machine
    data_pattern = os.path.join(base_dir, "data", target_machine, "*_train.csv")
    csv_files = sorted(glob.glob(data_pattern))

    if not csv_files:
        logger.warning(f"⚠️ CSV 파일을 찾지 못했습니다: {data_pattern}")
        return

    for csv_path in csv_files:
        current_machine = os.path.basename(os.path.dirname(csv_path))
        logger.info(f"📂 읽는 중: {csv_path}")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                numeric_row = {k: try_parse_number(k, v) for k, v in row.items()}
                numeric_row["label"] = 0.0
                numeric_row["send_timestamp"] = datetime.now().isoformat()
                numeric_row["machine"] = current_machine
                numeric_row["usage"] = f"train"
                yield numeric_row, current_machine # 이름도 반환

def to_str(x):
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    return str(x)

def try_parse_number(key, value):
    """
    특정 컬럼(col_0~col_37, label)만 숫자로 파싱하고
    timestamp 같은 컬럼은 그대로 string 유지.
    """
    if key in ('timestamp', 'usage', 'machine'):
        return to_str(value) # 반드시 문자열 유지
    
    if key in ('label'):
        return int(value) # 반드시 문자열 유지

    if key in {f"col_{i}" for i in range(38)}:
        return float(value)  # 숫자 변환 필요 없는 컬럼

    # 이제 숫자로 변환 대상인 경우만 아래 진행
    try:
        if "." in value or "e" in value or "E" in value:
            return float(value)
        else:
            return int(value)
    except Exception:
        return value


# -------------------- Kafka 전송 콜백 -------------------- #
def on_send_success(record_metadata):
    logger.info(f"✅ 성공: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

def on_send_error(excp):
    logger.error(f"❌ 실패: {excp}")


# -------------------- 메인 카프카 루프 -------------------- #
def main_kafka(args):
    global logger

    bootstrap_servers = args.bootstrap_servers.split(",")
    topic_name = args.topic

    # ✅ 토픽 자동 생성
    create_topic(
        bootstrap_servers, 
        topic_name, 
        num_partitions=args.partitions, 
        replication_factor=args.replications
    )

    # ✅ Kafka Producer 설정 (지연 최소화, 병렬 최적화)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
        key_serializer=str.encode,
        acks='all',                   # 완전 보장
        # retries=3,
        # linger_ms=1000,                  # 즉시 전송
        batch_size=16384,
        # request_timeout_ms=20000
        # client_id="backfill-producer",
        # bootstrap_servers=bootstrap_servers,
        # key_serializer=str.encode,
        # value_serializer=json_serializer,
        # acks='1',  # 속도 ↑ (acks=all 보다 빠름)
        # # linger_ms=5,  # 배치 대기 시간 (5ms)
        # # batch_size=32768,  # 32KB
        # # compression_type='gzip',  # CPU 부하 적고 빠름
        # # max_in_flight_requests_per_connection=5
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))
    total_sent = 0
    start_time = time.time()
    
    logger.info("🚀 Kafka Producer 시작 (모든 machine-* CSV 병렬 전송). Ctrl+C로 종료.")

    try:
        for record, machine in iter_all_csv_rows(args.machine, base_dir):
            future = producer.send(
                topic_name,
                key=f"{machine}",  # 파티션 균등 분산을 위한 key
                value=record
            )
            future.add_callback(on_send_success).add_errback(on_send_error)
            total_sent += 1
            if total_sent % args.batch_size == 0:
                producer.flush()

        producer.flush()
        elapsed = time.time() - start_time
        logger.info(f"\n✅ 전송 완료: {total_sent} rows / {elapsed:.2f}초 / 평균 {total_sent/elapsed:.1f} msg/sec")

    except KeyboardInterrupt:
        logger.error("🛑 프로듀서 종료 (수동 중단)")
    finally:
        producer.flush()
        producer.close()

# -------------------- 메인 postgresql 루프 -------------------- #
def main_postgres(args):
    global logger

    """CSV 데이터를 PostgreSQL에 직접 저장"""
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # ✅ 환경변수 로드 (.env)
    load_dotenv()

    PG_HOST = args.pg_host
    PG_PORT = args.pg_port
    PG_DB = args.pg_db
    PG_USER = args.pg_user
    PG_PASS = args.pg_pass
    PG_TABLE = args.pg_table

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    # ✅ 테이블 생성 (없으면 자동 생성), PK 자동증가 ID는 삭제함: id SERIAL PRIMARY KEY,
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {PG_TABLE} (
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
    conn.commit()

    total_inserted = 0
    start_time = time.time()
    logger.info(f"🚀 PostgreSQL 저장 시작: {PG_HOST}:{PG_PORT}/{PG_DB} → {PG_TABLE}")

    try:
        batch = []
        for record, machine in iter_all_csv_rows(args.machine, base_dir):
            cols = ["send_timestamp", "machine", "timestamp", "usage", "label"] + [f"col_{i}" for i in range(38)]
            values = [record.get(c, None) for c in cols]
            batch.append(values)

            if len(batch) >= 500:
                placeholders = ",".join(["%s"] * len(cols))
                insert_sql = f"INSERT INTO {PG_TABLE} ({','.join(cols)}) VALUES ({placeholders})"
                execute_batch(cur, insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"💾 {total_inserted} rows inserted...")
                batch.clear()

        if batch:
            placeholders = ",".join(["%s"] * len(cols))
            insert_sql = f"INSERT INTO {PG_TABLE} ({','.join(cols)}) VALUES ({placeholders})"
            execute_batch(cur, insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)
            batch.clear()

        elapsed = time.time() - start_time
        logger.info(f"\n✅ 저장 완료: {total_inserted} rows / {elapsed:.2f}초 / 평균 {total_inserted/elapsed:.1f} row/sec")

    except KeyboardInterrupt:
        logger.error("🛑 수동 종료")
    finally:
        cur.close()
        conn.close()

# -------------------- 실행 -------------------- #
if __name__ == "__main__":
    logger = setup_logger()

    parser = argparse.ArgumentParser(description='Kafka 또는 PostgreSQL - 모든 machine CSV 전송')
    parser.add_argument('--dest', choices=['postgresql', 'kafka'], default='kafka', type=str, help='메시지를 보낼 곳 (postgresql, kafka)')
    parser.add_argument('--topic', default='backfill-train-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                        type=str, help='Kafka 또는 Postgres 서버')
    parser.add_argument('--partitions', default=14, type=int, help='토픽 파티션 수 (기본: 14)')
    parser.add_argument('--replications', default=1, type=int, help='토픽 복제본 수 (기본: 1)')
    parser.add_argument('--batch-size', default=1000, type=int, help='메시지 전송 배치 사이즈')
    
    # ✅ PostgreSQL 인자 추가
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"), help='PostgreSQL 호스트명')
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", 5432)), help='PostgreSQL 포트')
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"), help='PostgreSQL DB명')
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"), help='PostgreSQL 사용자명')
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"), help='PostgreSQL 비밀번호')
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_table_realtime"), help='PostgreSQL 테이블명')
    
    parser.add_argument('--machine', default='all', type=str, help='측정할 머신 이름 ex. machine-*-*')

    args = parser.parse_args()

    if args.dest == 'kafka':
        logger.info("Kafka 전송 시작")
        main_kafka(args)
    elif args.dest == 'postgresql':
        logger.info("PostgreSQL 저장 시작")
        main_postgres(args)
    else:
        logger.error(f"❌ 잘못된 dest 인자: {args.dest}")
        sys.exit(1)
