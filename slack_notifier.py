import requests
import os

def send_slack_notification(config_key, full_table_id, success, error_message=None):
    """
    Sends a notification to a Slack channel using a webhook.

    Args:
        config_key (str): The configuration key used for the operation.
        full_table_id (str): The full BigQuery table ID (project.dataset.table).
        success (bool): Whether the operation was successful.
        error_message (str, optional): The error message if the operation failed. Defaults to None.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("SLACK_WEBHOOK_URL environment variable not set. Skipping Slack notification.")
        return

    if success:
        message = f"✅ [{config_key}] 성공: BigQuery 테이블 {full_table_id}에 로드했습니다."
    else:
        message = f"❌ [{config_key}] 실패: BigQuery 테이블 {full_table_id} 로드 중 오류 발생.\n오류: {error_message}"

    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Slack notification sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Slack notification: {e}")