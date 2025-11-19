#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
이메일 알림 모듈
SMTP를 사용하여 머신 이상감지 알림을 이메일로 전송합니다.
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import os
from datetime import datetime
# from dotenv import load_dotenv
import logging

# .env 파일 불러오기 (기본 경로: 현재 실행 디렉토리)
# load_dotenv()

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from utils.log_utils import setup_logger
# from config.config import EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT


# -------------------- 로거 전역 선언 -------------------- #
logger = None

def setup_logger():
    """로거 설정: 콘솔 + 파일 출력"""
    # 로그 디렉터리 생성
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 파일명: 실행 시각 기반
    log_filename = os.path.join(log_dir, f"email_notifier_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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



class EmailNotifier:
    """이메일 알림 클래스"""
    
    def __init__(self, sender=None, password=None, recipient=None, smtp_server=None, smtp_port=None):
        """초기화 함수"""
        self.sender = sender # os.getenv("EMAIL_SENDER", "") # sender or EMAIL_SENDER
        self.password = password # os.getenv("EMAIL_PASSWORD", "") # password or EMAIL_PASSWORD
        self.recipient = recipient # os.getenv("EMAIL_RECIPIENT", "") # recipient or EMAIL_RECIPIENT
        self.smtp_server = smtp_server # os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com") # smtp_server or EMAIL_SMTP_SERVER or "smtp.gmail.com"
        self.smtp_port = smtp_port # os.getenv("EMAIL_SMTP_PORT", 587) # smtp_port or EMAIL_SMTP_PORT or 587
        
        if not self.sender or not self.password or not self.recipient:
            logger.warning("이메일 설정이 완료되지 않았습니다. 이메일 알림이 비활성화됩니다.")
        else:
            logger.info(f"이메일 알림 초기화 완료. 발신자: {self.sender}, 수신자: {self.recipient}")

    def send_email(self, subject, html_content, text_content=None):
        """이메일 전송"""
        if not self.sender or not self.password or not self.recipient:
            logger.warning("이메일 설정이 완료되지 않아 이메일을 전송할 수 없습니다.")
            return False
        
        # 수신자가 문자열이면 리스트로 변환
        recipients = self.recipient if isinstance(self.recipient, list) else [self.recipient]
        
        # 이메일 메시지 생성
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.sender
        message["To"] = ", ".join(recipients)
        
        # 일반 텍스트와 HTML 버전의 이메일 내용 추가
        if text_content:
            message.attach(MIMEText(text_content, "plain"))
        message.attach(MIMEText(html_content, "html"))
        
        try:
            # SMTP 서버 연결 및 로그인
            context = ssl.create_default_context()
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(self.sender, self.password)
                
                # 이메일 전송
                server.sendmail(self.sender, recipients, message.as_string())
                
            logger.info(f"이메일 전송 성공: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 중 오류 발생: {e}")
            return False
    
    def send_anomaly_alert(self, machine, zscore, message):
        """이상감지 알림 이메일 전송"""
        # 이상치 통계량에 따른 이모지와 색상 설정
        emoji = "🚀🚀🚀" if abs(zscore) > 2.57 else "🚀" if abs(zscore) > 1.96 else "📉"
        color = "#dc3545" if abs(zscore) > 2.57 else "#fef607" if abs(zscore) > 1.96 else "#1f9a00"
        
        # 이메일 제목
        subject = f"{emoji} {machine} 이상감지 알림: {abs(zscore):.2f}"
        
        # HTML 이메일 내용
        html_content = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                    .header {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; text-align: center; }}
                    .content {{ padding: 20px 0; }}
                    .price-box {{ padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px; }}
                    .price-change {{ color: {color}; font-weight: bold; }}
                    .footer {{ font-size: 12px; color: #6c757d; border-top: 1px solid #e9ecef; padding-top: 10px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h2>{emoji} {machine} 이상감지 알림</h2>
                    </div>
                    <div class="content">
                        <div class="price-box">
                            <p><strong>Z-Score:</strong> <span class="price-change">{abs(zscore):.2f}</span></p>
                        </div>
                        <p>{message}</p>
                    </div>
                    <div class="footer">
                        <p>이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.</p>
                        <p>머신 이상감지 알림 파이프라인 - 자동 생성 메일입니다.</p>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # 일반 텍스트 이메일 내용
        text_content = f"""
        {machine} 이상감지 알림
        
        Z-Score: ${abs(zscore):.2f}
        
        {message}
        
        이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}에 생성되었습니다.
        머신 이상감지 파이프라인 - 자동 생성 메일입니다.
        """
        
        return self.send_email(subject, html_content, text_content)
    
if __name__ == "__main__":
    pass