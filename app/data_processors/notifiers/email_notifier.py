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

import logging
logger = logging.getLogger("anomaly_processor")   # ★ 단순히 같은 이름으로 가져오기

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
        
    def classify_severity(self, z):
        """Z-score 기반 severity 분류"""
        az = abs(z)
        if az >= 3.0:
            return "CRITICAL", "🚨🔥", "#d9534f"    # 강한 이상
        elif az >= 2.0:
            return "WARNING", "⚠️", "#f0ad4e"     # 경고
        else:
            return "NOTICE", "🔎", "#0275d8"      # 경미한 변동
        
    def send_anomaly_alert(self, machine, zscore, message):
        """이상감지 알림 이메일 전송"""
        # # 이상치 통계량에 따른 이모지와 색상 설정
        # emoji = "🚀🚀🚀" if abs(zscore) > 2.57 else "🚀" if abs(zscore) > 1.96 else "📉"
        # color = "#dc3545" if abs(zscore) > 2.57 else "#0707fe" if abs(zscore) > 1.96 else "#1f9a00"
        
        # # 이메일 제목
        # subject = f"{emoji} {machine} 이상감지 알림: {abs(zscore):.2f}"

        severity, emoji, color = self.classify_severity(zscore)

        # 이메일 제목
        subject = f"[{severity}] {emoji} {machine} Z-score={abs(zscore):.2f} | 이상 감지"

        # HTML 이메일 내용
        html_content = f"""
        <html>
        <body style="margin:0; padding:0; background-color:#f5f6fa; font-family:Arial, sans-serif;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f5f6fa; padding:20px 0;">
            <tr>
                <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; padding:20px; border:1px solid #e0e0e0;">
                    
                    <!-- Header -->
                    <tr>
                    <td align="center" style="padding:15px; background-color:#f0f2f5; border-radius:6px;">
                        <h2 style="margin:0; color:#333333; font-size:22px; font-weight:bold;">
                        {emoji} {machine} 이상감지 알림
                        </h2>
                    </td>
                    </tr>

                    <!-- Z-score box -->
                    <tr>
                    <td style="padding:20px 0;">
                        <table width="100%" cellpadding="12" cellspacing="0" 
                            style="background-color:#fafafa; border-radius:6px; border:1px solid #e6e6e6;">
                        <tr>
                            <td style="font-size:16px; color:#333;">
                            <strong>Z-Score: </strong>
                            <span style="color:{color}; font-weight:bold; font-size:20px;">
                                {abs(zscore):.2f}
                            </span>
                            </td>
                        </tr>
                        </table>
                    </td>
                    </tr>

                    <!-- Message area -->
                    <tr>
                    <td style="padding:15px 0; font-size:15px; color:#333;">
                        {message}
                    </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                    <td style="padding-top:25px; font-size:12px; color:#777; border-top:1px solid #e6e6e6;">
                        <p style="margin:6px 0;">
                        이 알림은 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 기준으로 생성되었습니다.
                        </p>
                        <p style="margin:6px 0;">
                        머신 이상감지 파이프라인 자동 생성 메일입니다.
                        </p>
                    </td>
                    </tr>

                </table>
                </td>
            </tr>
            </table>
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