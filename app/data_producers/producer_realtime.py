#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import csv
import json
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# logger 파일로 저장
# try catch 자세하게 적용

# -------------------- 토픽 생성 -------------------- #
def create_topic(bootstrap_servers, topic_name, num_partitions=3, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic_creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ 토픽 생성 완료: {topic_name}")
    except TopicAlreadyExistsError:
        print(f"⚠️ 토픽 '{topic_name}'은 이미 존재합니다.")
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
        print(f"⚠️ CSV 파일을 찾지 못했습니다: {data_pattern}")
        return

    for csv_path in csv_files:
        print(f"📂 읽는 중: {os.path.basename(csv_path)}")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # CSV의 timestamp 대신 전송 시각을 덮어쓰기 (선택)
                numeric_row["send_timestamp"] = datetime.now().isoformat()
                numeric_row["machine"] = f"{machine}-test"
                # 각 행을 float 또는 int로 변환
                numeric_row = {k: try_parse_number(v) for k, v in row.items()}
                yield numeric_row


def try_parse_number(value):
    """문자열을 float/int로 변환, 실패 시 그대로 반환"""
    try:
        if "." in value or "e" in value or "E" in value:
            return float(value)
        else:
            return int(value)
    except Exception:
        return value


# -------------------- Kafka 전송 콜백 -------------------- #
def on_send_success(record_metadata):
    print(f"✅ 성공: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

def on_send_error(excp):
    print(f"❌ 실패: {excp}")


# -------------------- 메인 루프 -------------------- #
def main():
    # 인자 파싱
    parser = argparse.ArgumentParser(description='Kafka 프로듀서 예제 - 메시지 생성')
    parser.add_argument('--topic', default='test-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--interval', default=60, type=int, help='메시지 전송 간격 (단위 초)')
    parser.add_argument('--machine', default='machine-1-1', type=str, help='측정할 머신 이름 ex. machine-*-*')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                     type=str, help='Kafka 부트스트랩 서버')
    args = parser.parse_args()
    
    bootstrap_servers = args.bootstrap_servers.split(",") # ['kafka.kafka.svc.cluster.local:9092']
    topic_name = args.topic # "realtime-test-topic"

    create_topic(
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        num_partitions=1,
        replication_factor=3
    )

    producer = KafkaProducer(
        client_id="machine-producer",   # ✅ 프로듀서 식별용 이름
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,         # ✅ 문자열 → bytes 자동 변환
        value_serializer=json_serializer,
        acks='all'
    )

    print("🚀 Kafka Producer 시작 (무한 반복). Ctrl+C로 종료.")

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

                print(f"📤 전송: {message}")
                time.sleep(args.interval)  # 전송 간격 조정 가능 (분당 1건)
            
            # 한 바퀴 다 돌았으면 대기 후 다시 시작
            print("🔁 CSV 전체 전송 완료. 60초 후 재시작...\n")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("🛑 프로듀서 종료")
    finally:
        producer.flush()
        producer.close()


# -------------------- 실행 -------------------- #
if __name__ == "__main__":

    main()
