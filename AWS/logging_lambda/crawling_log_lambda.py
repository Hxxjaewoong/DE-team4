import json
import requests
import os
from datetime import datetime, timedelta

# 환경 변수에서 Slack Webhook URL 가져오기 (AWS Lambda 환경 변수에 등록 필수)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_slack_alert(log_data):
    """Slack Webhook으로 오류 알림 전송"""
    if not SLACK_WEBHOOK_URL:
        print("❌ Webhook URL이 설정되지 않았습니다.")
        return {"status": "failed", "error": "Missing SLACK_WEBHOOK_URL"}

    # Slack 메시지 구성
    message = (
        f"🚨 *오류 발생!*\n"
        f"- 발생 시간: {log_data.get('timestamp', (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M'))}\n"
        f"- 위치: `{log_data.get('source', '알 수 없음')}`\n"
        f"- 단계: `{log_data.get('stage', '알 수 없음')}`\n"
        f"- 키워드: `{log_data.get('keyword', 'N/A')}`\n"
        f"- URL: {log_data.get('url', '없음')}\n"
        f"- 오류 메시지: ```{log_data.get('error', 'No details')}```\n"
    )

    payload = {
        "text": message
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        print("✅ Slack 알림 전송 성공!")
        return {"status": "success"}
    else:
        print(f"❌ Slack 메시지 전송 실패: {response.status_code}, 응답: {response.text}")
        return {"status": "failed", "error": response.text}

def lambda_handler(event, context):
    """Step Function에서 오류 로그를 받아 Slack 알림 전송"""
    try:
        print(f"🟢 수신된 이벤트: {json.dumps(event, ensure_ascii=False)}")

        # 오류 로그가 포함된 경우 Slack으로 전송
        if "error" in event:
            result = send_slack_alert(event)
            return result
        else:
            print("⚠️ 오류 정보가 없는 이벤트입니다.")
            return {"status": "no_error_logged"}

    except Exception as e:
        print(f"❌ Lambda 실행 중 오류 발생: {str(e)}")
        return {"status": "failed", "error": str(e)}
