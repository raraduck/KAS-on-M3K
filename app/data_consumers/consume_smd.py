#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json
from datetime import datetime

# -------------------- JSON 역직렬화 -------------------- #
def json_deserializer(data):
    """바이트를 JSON 객체로 역직렬화"""
    return json.loads(data.decode('utf-8'))

# -------------------- 메시지 처리 -------------------- #
def process_message(message: dict):
    """SMD 메시지를 처리하는 함수"""
    try:
        timestamp = message.get("timestamp") or message.get("send_timestamp")
        label = message.get("label", "N/A")

        # 주요 feature 요약
        cols = [v for k, v in message.items() if k.startswith("col_")]
        avg_val = round(sum(cols) / len(cols), 5) if cols else None

        print(f"🕓 {timestamp} | label={label} | mean(col_*)={avg_val}")

        # 원한다면 anomaly 표시
        if label == 1:
            print("🚨 이상 탐지! (label=1)")
    except Exception as e:
        print(f"⚠️ 메시지 파싱 오류: {e}")
        print(f"원본 메시지: {message}")

# -------------------- 메인 -------------------- #
def main():
    consumer = KafkaConsumer(
        'server-machine-usage',               # ✅ SMD 토픽
        bootstrap_servers='kafka.kafka.svc.cluster.local:9092',
        auto_offset_reset='earliest',         # 처음부터 읽기
        enable_auto_commit=True,
        group_id='smd-consumer-group',
        value_deserializer=json_deserializer,
        consumer_timeout_ms=10000                 # 무한 대기
    )

    print("🚀 Kafka SMD Consumer 시작. Ctrl+C로 종료.")
    print("'server-machine-usage' 토픽 메시지 대기 중...\n")

    try:
        for message in consumer:
            print(f"\n📩 파티션={message.partition}, 오프셋={message.offset}")
            process_message(message.value)
    except KeyboardInterrupt:
        print("\n🛑 컨슈머 종료")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
