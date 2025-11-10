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
def create_topic(bootstrap_servers, topic_name, num_partitions=14, replication_factor=3):
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
        print(f"✅ 토픽 생성 완료: {topic_name} (partitions={num_partitions}, replicas={replication_factor})")
    except TopicAlreadyExistsError:
        print(f"⚠️ 토픽 '{topic_name}'은 이미 존재합니다.")
    finally:
        admin_client.close()


# -------------------- JSON 직렬화 -------------------- #
def json_serializer(data):
    """데이터를 JSON 형식으로 직렬화"""
    return json.dumps(data).encode('utf-8')


# -------------------- CSV 파일 제너레이터 -------------------- #
def iter_all_csv_rows(base_dir):
    """
    data/machine-*-*/*_train.csv 파일을 전부 순회하며 각 row yield
    """
    data_pattern = os.path.join(base_dir, "data", "machine-*", "*_train.csv")
    csv_files = sorted(glob.glob(data_pattern))

    if not csv_files:
        print(f"⚠️ CSV 파일을 찾지 못했습니다: {data_pattern}")
        return

    for csv_path in csv_files:
        machine = os.path.basename(os.path.dirname(csv_path))
        print(f"📂 읽는 중: {csv_path}")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                numeric_row = {k: try_parse_number(v) for k, v in row.items()}
                numeric_row["send_timestamp"] = datetime.now().isoformat()
                numeric_row["machine"] = f"{machine}-train"
                yield numeric_row, machine  # machine 이름도 반환


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
    parser = argparse.ArgumentParser(description='Kafka 프로듀서 - 모든 machine CSV 전송')
    parser.add_argument('--topic', default='backfill-train-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                        type=str, help='Kafka 부트스트랩 서버')
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(",")
    topic_name = args.topic

    # ✅ 토픽 자동 생성
    create_topic(bootstrap_servers, topic_name, num_partitions=14, replication_factor=3)

    # ✅ Kafka Producer 설정 (지연 최소화, 병렬 최적화)
    producer = KafkaProducer(
        client_id="backfill-producer",
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,
        value_serializer=json_serializer,
        acks='1',  # 속도 ↑ (acks=all 보다 빠름)
        # linger_ms=5,  # 배치 대기 시간 (5ms)
        # batch_size=32768,  # 32KB
        # compression_type='gzip',  # CPU 부하 적고 빠름
        # max_in_flight_requests_per_connection=5
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))
    total_sent = 0
    start_time = time.time()
    
    print("🚀 Kafka Producer 시작 (모든 machine-* CSV 병렬 전송). Ctrl+C로 종료.")

    try:
        for record, machine in iter_all_csv_rows(base_dir):
            future = producer.send(
                topic_name,
                key=f"{machine}-train",  # 파티션 균등 분산을 위한 key
                value=record
            )
            future.add_callback(on_send_success).add_errback(on_send_error)
            total_sent += 1

        producer.flush()
        elapsed = time.time() - start_time
        print(f"\n✅ 전송 완료: {total_sent} rows / {elapsed:.2f}초 / 평균 {total_sent/elapsed:.1f} msg/sec")

    except KeyboardInterrupt:
        print("🛑 프로듀서 종료 (수동 중단)")
    finally:
        producer.flush()
        producer.close()


# -------------------- 실행 -------------------- #
if __name__ == "__main__":

    main()
