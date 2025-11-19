#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
이상감지 알림 프로세서 모듈
Kafka에서 이상감지 데이터를 소비하고 설정된 조건에 따라 알림을 생성합니다.
"""

import sys
import os

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from config.config import (
#     STOCK_PRICES_TOPIC, 
#     PRICE_CHANGE_THRESHOLD, STOCK_TICKERS
# )
# from utils.kafka_utils import create_consumer
# from utils.log_utils import setup_logger


import json
from kafka import KafkaConsumer
# from notifiers.slack_notifier import SlackNotifier
from notifiers.email_notifier import EmailNotifier
from datetime import datetime
import argparse
from dotenv import load_dotenv
import logging

OUTLIER_THRESHOLD = 1.96
MACHINES=[
    'machine-1-1', 
    'machine-1-2', 
    'machine-1-3', 
    'machine-1-4', 
    'machine-1-5', 
    'machine-1-6', 
    'machine-1-7', 
    'machine-1-8', 
    'machine-2-1', 
    'machine-2-2', 
    'machine-2-3', 
    'machine-2-4', 
    'machine-2-5', 
    'machine-2-6', 
    'machine-2-7', 
    'machine-2-8', 
    'machine-2-9', 
    'machine-3-1', 
    'machine-3-2', 
    'machine-3-3', 
    'machine-3-4', 
    'machine-3-5', 
    'machine-3-6', 
    'machine-3-7', 
    'machine-3-8', 
    'machine-3-9', 
    'machine-3-10',
    'machine-3-11' 
]

# .env 파일 불러오기 (기본 경로: 현재 실행 디렉토리)
load_dotenv()

# -------------------- 로거 전역 선언 -------------------- #
logger = None

def setup_logger(logger_name="anomaly_processor"):
    """로거 설정: 콘솔 + 파일 출력"""
    # 로그 디렉터리 생성
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 파일명: 실행 시각 기반
    log_filename = os.path.join(log_dir, f"{logger_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    # 로거 생성
    logger_obj = logging.getLogger(logger_name)
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


# -------------------- JSON 역직렬화 -------------------- #
def json_deserializer(data):
    """Kafka 메시지를 JSON으로 역직렬화"""
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        logger.warning(f"⚠️ JSON 디코딩 오류: {e}")
        return None
    
def create_consumer(args, auto_offset_reset='latest', enable_auto_commit=True):
    """Kafka Consumer 생성"""
    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_servers.split(","),
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            group_id=args.group_id,
            value_deserializer=json_deserializer, # lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Kafka Consumer 연결 성공: {args.bootstrap_servers}, 토픽: {args.topic}")
        return consumer
    except Exception as e:
        logger.error(f"Kafka Consumer 연결 실패: {e}")
        return None


class AnomalyAlert:
    """알림 프로세서 클래스"""
    
    def __init__(self, args):
        """초기화 함수"""
        self.consumer = create_consumer(args)
        # self.slack_notifier = SlackNotifier()
        # EMAIL_SENDER
        # EMAIL_PASSWORD
        # EMAIL_RECIPIENT
        # EMAIL_SMTP_SERVER
        # EMAIL_SMTP_PORT
        self.email_notifier = EmailNotifier(
            args.email_sender, 
            args.email_password, 
            args.email_recipient,
            args.email_smtp_server,
            args.email_smtp_port
        )
        self.alert_thresholds = {}  # 머신별 알림 임계값 (기본값은 PRICE_CHANGE_THRESHOLD 사용)
        
        # 기본 알림 임계값 설정
        for machine in MACHINES:
            self.alert_thresholds[machine] = OUTLIER_THRESHOLD
            
        logger.info(f"이상감지 알림 프로세서 초기화 완료. 알림 임계값: {self.alert_thresholds}")
    
    # def set_alert_threshold(self, ticker, threshold):
    #     """특정 종목의 알림 임계값 설정"""
    #     self.alert_thresholds[ticker] = threshold
    #     logger.info(f"종목 {ticker}의 알림 임계값을 {threshold}%로 설정")
    
    def process_message(self, message):
        """메시지 처리"""
        try:
            send_timestamp = message['send_timestamp']
            machine = message['machine']
            timestamp = message['timestamp']
            zscore = message['zscore']
            
            # 임계값 초과 여부 확인
            threshold = self.alert_thresholds.get(machine, OUTLIER_THRESHOLD)
            
            if abs(zscore) >= threshold:
                # 알림 메시지 생성
                # direction = "상승" if change_pct > 0 else "하락"
                message_text = f"{machine} 에서 z-score 가 {threshold} 을 벗어나 이상현상이 감지되었습니다: {abs(zscore)}"
                
                # 알림 전송
                logger.info(f"이상감지 알림 발생: {message_text}")
                # self.slack_notifier.send_price_alert(ticker, current_price, change_pct, message_text)
                self.email_notifier.send_anomaly_alert(machine, zscore, message_text)
            
        except Exception as e:
            logger.error(f"메시지 처리 중 오류 발생: {e}")
    
    def run(self):
        """알림 프로세서 실행"""
        logger.info("이상감지 알림 프로세서 시작")
        
        try:
            for message in self.consumer:  # 무한 루프 - Kafka에서 메시지가 오면 처리
                self.process_message(message.value)
        except KeyboardInterrupt:
            logger.info("사용자에 의해 프로그램이 종료되었습니다.")
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka Consumer 연결 종료")

def main():
    global logger
    logger = setup_logger("anomaly_processor")   # logger 객체 생성

    parser = argparse.ArgumentParser(description="Kafka → PostgreSQL Consumer")

    # Kafka 설정
    parser.add_argument('--topic', default='test-topic', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--bootstrap-servers', default='kafka.kafka.svc.cluster.local:9092',
                     type=str, help='Kafka 부트스트랩 서버')
    parser.add_argument("--group-id", default="smd-realtime-group", help="Kafka consumer group ID")

    # PostgreSQL 설정
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", 5432)))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "postgres"))
    parser.add_argument("--pg-table", default=os.getenv("PG_TABLE", "smd_table_realtime"))

    parser.add_argument("--email-sender", default=os.getenv("EMAIL_SENDER", "")) # sender or EMAIL_SENDER
    parser.add_argument("--email-password", default=os.getenv("EMAIL_PASSWORD", "")) # password or EMAIL_PASSWORD
    parser.add_argument("--email-recipient", default=os.getenv("EMAIL_RECIPIENT", "")) # recipient or EMAIL_RECIPIENT
    parser.add_argument("--email-smtp-server", default=os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")) # smtp_server or EMAIL_SMTP_SERVER or "smtp.gmail.com"
    parser.add_argument("--email-smtp-port", default=os.getenv("EMAIL_SMTP_PORT", 587)) # smtp_port or EMAIL_SMTP_PORT or 587
        

    # parser.add_argument("--batch-size", type=int, default=100, help="Postgres로 저장할 batch 크기")

    args = parser.parse_args()

    """메인 함수"""
    processor = AnomalyAlert(args)
    processor.run()

if __name__ == "__main__":
    main() 