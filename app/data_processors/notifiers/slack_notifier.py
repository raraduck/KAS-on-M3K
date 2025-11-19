#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slack 알림 모듈
Slack 웹훅을 사용하여 주식 가격 알림을 전송합니다.
"""

import json
import requests
import logging
import sys
from datetime import datetime
import os

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log_utils import setup_logger
from config.config import SLACK_WEBHOOK_URL, SLACK_CHANNEL

# 로깅 설정
logger = setup_logger("slack_notifier", "logs/slack_notifier.log")

class SlackNotifier:
    """Slack 알림 클래스"""
    
    def __init__(self, webhook_url=None, channel=None):
        """초기화 함수"""
        self.webhook_url = webhook_url or SLACK_WEBHOOK_URL
        self.channel = channel or SLACK_CHANNEL
        
        if not self.webhook_url:
            logger.warning("Slack Webhook URL이 설정되지 않았습니다. Slack 알림이 비활성화됩니다.")
        else:
            logger.info(f"Slack 알림 초기화 완료. 채널: {self.channel}")
    
    def send_message(self, blocks, text="주식 알림"):
        """Slack 메시지 전송"""
        if not self.webhook_url:
            logger.warning("Slack Webhook URL이 설정되지 않아 메시지를 전송할 수 없습니다.")
            return False
        
        try:
            payload = {
                "channel": self.channel,
                "text": text,
                "blocks": blocks
            }
            
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info("Slack 메시지 전송 성공")
                return True
            else:
                logger.error(f"Slack 메시지 전송 실패: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Slack 메시지 전송 중 오류 발생: {e}")
            return False
    
    def send_price_alert(self, ticker, price, change_pct, message):
        """주식 가격 알림 전송"""
        # 변동 방향에 따른 이모지 설정
        emoji = "🚀" if change_pct > 0 else "📉"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {ticker} 가격 알림"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*현재가:* ${price:.2f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*변동률:* {change_pct:.2f}%"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"알림 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]
        
        return self.send_message(blocks, f"{ticker} 가격 알림: {change_pct:.2f}% {emoji}")
    
    def send_volume_alert(self, ticker, volume, avg_volume, pct_change, message):
        """거래량 알림 전송"""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 {ticker} 거래량 알림"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*현재 거래량:* {volume:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*평균 거래량:* {avg_volume:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*변동률:* {pct_change:.2f}%"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            }
        ]
        
        return self.send_message(blocks, f"{ticker} 거래량 알림: {pct_change:.2f}% 증가")
    
    def send_daily_summary(self, summary_data):
        """일일 요약 보고서 Slack 전송"""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📅 주식 일일 요약 보고서: {summary_data['date']}",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            }
        ]
        
        # 종목별 요약 정보 추가
        for ticker_data in summary_data['tickers']:
            ticker = ticker_data['ticker']
            change_pct = ticker_data['change_pct']
            emoji = "🚀" if change_pct > 0 else "📉" if change_pct < 0 else "➖"
            sign = "+" if change_pct > 0 else ""
            
            # RSI 상태 이모지
            rsi_value = ticker_data.get('rsi', 0)
            rsi_status = ticker_data.get('rsi_status', 'unknown')
            rsi_emoji = "⚠️" if rsi_status == 'overbought' else "✅" if rsi_status == 'oversold' else "➖"
            
            # 이동평균선 상태 이모지
            ma_status = ticker_data.get('ma_status', 'unknown')
            ma_emoji = "📈" if ma_status == 'bullish' else "📉" if ma_status == 'bearish' else "➖"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{ticker}* {emoji} {sign}{change_pct:.2f}% | RSI: {rsi_emoji} {rsi_value:.1f} | MA: {ma_emoji} {ma_status}"
                }
            })
            
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"시가: ${ticker_data['open']:.2f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"종가: ${ticker_data['close']:.2f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"고가: ${ticker_data['high']:.2f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"저가: ${ticker_data['low']:.2f}"
                    }
                ]
            })
        
        # 주요 알림 요약 추가
        if summary_data.get('alerts'):
            blocks.append({
                "type": "divider"
            })
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*오늘의 주요 알림*"
                }
            })
            
            for alert in summary_data['alerts']:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"• {alert}"
                    }
                })
                
        # 범례 추가
        blocks.append({
            "type": "divider"
        })
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "*범례:* RSI ⚠️ 과매수 / ✅ 과매도 | MA 📈 강세 / 📉 약세"
                }
            ]
        })
        
        return self.send_message(blocks, "주식 일일 요약 보고서")

# 테스트 코드
if __name__ == "__main__":
    from datetime import datetime
    
    notifier = SlackNotifier()
    
    # 테스트 가격 알림
    notifier.send_price_alert("AAPL", 175.25, 2.5, "애플 주가가 2% 이상 상승했습니다.")
    
    # 테스트 거래량 알림
    notifier.send_volume_alert("AAPL", 80000000, 65000000, 23.08, "애플 거래량이 평균보다 20% 이상 증가했습니다.")
    
    # 테스트 일일 요약
    summary_data = {
        "date": "2023-05-01",
        "tickers": [
            {
                "ticker": "AAPL",
                "open": 170.25,
                "close": 175.25,
                "high": 176.30,
                "low": 169.50,
                "change_pct": 2.5
            },
            {
                "ticker": "MSFT",
                "open": 350.10,
                "close": 345.75,
                "high": 352.20,
                "low": 344.80,
                "change_pct": -1.2
            }
        ],
        "alerts": [
            "AAPL 주가 2.5% 상승",
            "MSFT 주가 1.2% 하락",
            "AAPL 거래량 23% 증가"
        ]
    }
    notifier.send_daily_summary(summary_data) 