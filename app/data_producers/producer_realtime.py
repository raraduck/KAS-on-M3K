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

# -------------------- 토픽 생성 -------------------- #
def create_topic(bootstrap_servers, topic_name, num_partitions=3, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic_creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        topic_configs={
            "retention.ms": "86400000", # 1일 = 24시간 * 60분 * 60초 * 1000 밀리초
            "cleanup.policy": "delete"
        }
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"✅ 토픽 생성 완료: {topic_name}")
    except TopicAlreadyExistsError:
        logger.warning(f"⚠️ 토픽 '{topic_name}'은 이미 존재합니다.")
    finally:
        admin_client.close()


# -------------------- JSON 직렬화 -------------------- #
def json_serializer(data):
    """데이터를 JSON 형식으로 직렬화"""
    return json.dumps(data).encode('utf-8')


# -------------------- CSV 파일 제너레이터 -------------------- #
def iter_smd_csv_rows(machine):
    """
    data/machine-*-*/ 하위의 *_test.csv 파일을 순회하며
    각 파일의 한 줄(row)을 yield
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_pattern = os.path.join(base_dir, "data", machine, "*_test.csv")
    csv_files = sorted(glob.glob(data_pattern))

    if not csv_files:
        logger.warning(f"⚠️ CSV 파일을 찾지 못했습니다: {data_pattern}")
        return

    for csv_path in csv_files:
        logger.info(f"📂 읽는 중: {os.path.basename(csv_path)}")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 각 행을 float 또는 int로 변환
                numeric_row = {k: try_parse_number(k, v) for k, v in row.items()}
                # CSV의 timestamp 대신 전송 시각을 덮어쓰기 (선택)
                numeric_row["send_timestamp"] = datetime.now().isoformat()
                numeric_row["machine"] = machine
                numeric_row["usage"] = f"test"
                yield numeric_row


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


# -------------------- 메인 루프 -------------------- #
def main():
    global logger
    # 인자 파싱
    parser = argparse.ArgumentParser(description='Kafka 프로듀서 예제 - 메시지 생성')
    parser.add_argument('--topic', default='test-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--interval', default=60, type=int, help='메시지 전송 간격 (단위 초)')
    parser.add_argument('--machine', default='machine-1-1', type=str, help='측정할 머신 이름 ex. machine-*-*')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                     type=str, help='Kafka 부트스트랩 서버')
    parser.add_argument('--partitions', default=1, type=int, help='토픽 파티션 수 (기본: 1)')
    parser.add_argument('--replications', default=3, type=int, help='토픽 복제본 수 (기본: 3)')
    args = parser.parse_args()
    
    bootstrap_servers = args.bootstrap_servers.split(",") # ['kafka.kafka.svc.cluster.local:9092']
    topic_name = args.topic # "realtime-test-topic"

    create_topic(
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        num_partitions=args.partitions,
        replication_factor=args.replications
    )

    producer = KafkaProducer(
        client_id="machine-producer",   # ✅ 프로듀서 식별용 이름
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,         # ✅ 문자열 → bytes 자동 변환
        value_serializer=json_serializer,
        acks='all'
    )

    logger.info("🚀 Kafka Producer 시작 (무한 반복). Ctrl+C로 종료.")

    try:
        while True:  # 🔁 무한 루프
            for message in iter_smd_csv_rows(args.machine):
                # Kafka로 전송
                future = producer.send(
                    topic_name, 
                    value=message, 
                    key=f"{args.machine}-test" # f"{args.machine}".encode("utf-8")
                )  # 반드시 bytes 형식
                future.add_callback(on_send_success).add_errback(on_send_error)

                logger.info(f"📤 전송: {message}")
                time.sleep(args.interval)  # 전송 간격 조정 가능 (분당 1건)
            
            # 한 바퀴 다 돌았으면 대기 후 다시 시작
            logger.info("🔁 CSV 전체 전송 완료. Inverval (ex. 60초) 후 재시작...\n")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.error("🛑 프로듀서 종료")
    finally:
        producer.flush()
        producer.close()


# -------------------- 실행 -------------------- #
if __name__ == "__main__":
    logger = setup_logger()

    main()
