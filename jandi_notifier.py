import os
import requests


def send_jandi_notification(config_key, full_table_id, success, num_rows=None, error_message=None):
    """
    Sends a notification to a Jandi webhook with the same content used for Slack.
    """
    webhook_url = os.environ.get("JANDI_WEBHOOK_URL")
    if not webhook_url:
        print("JANDI_WEBHOOK_URL environment variable not set. Skipping Jandi notification.")
        return

    if success:
        message = f"✅ [{config_key}] 성공:\n- BQ테이블 명 : {full_table_id}"
        if num_rows is not None:
            message += f"\n- 추가된 행개수 : {num_rows}개"
    else:
        message = (
            f"❌ [{config_key}] 실패: BigQuery 테이블 {full_table_id} 로드 중 오류 발생.\n"
            f"오류 내용 : {error_message}"
        )

    payload = {
        "body": message
    }
    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(webhook_url, headers=headers, json=payload)
        response.raise_for_status()
        print("Jandi notification sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Jandi notification: {e}")
