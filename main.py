import os
import json
import time
import pandas as pd
import gspread
from flask import Flask, request, Response
from google.auth import default
from google.cloud import bigquery
from slack_notifier import send_slack_notification
from jandi_notifier import send_jandi_notification

app = Flask(__name__)

def get_google_credentials():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/bigquery'
    ]
    credentials, project = default(scopes=scopes)
    return credentials

# ✅ 추가: Sheets API 503 재시도 함수
def get_sheet_data_with_retry(worksheet, column_range, config_key, max_retries=5, wait_sec=30):
    for attempt in range(1, max_retries + 1):
        try:
            return worksheet.get(column_range)
        except gspread.exceptions.APIError as e:
            status_code = e.response.status_code
            if status_code == 503 and attempt < max_retries:
                print(f"[{config_key}] Sheets API 503 오류 - {attempt}/{max_retries}번째 재시도 ({wait_sec}초 후)...", flush=True)
                time.sleep(wait_sec)
            else:
                raise  # 503 아닌 에러 or 재시도 소진 시 바로 raise
    raise Exception(f"[{config_key}] Sheets API 503 - {max_retries}회 재시도 후 최종 실패")


def load_sheet_to_bigquery(config_key: str):

    # 1. 설정 파일 로드
    try:
        with open('configs/main_configs.json', 'r', encoding='utf-8') as f:
            all_configs = json.load(f)
        
        config = all_configs.get(config_key)
        if not config:
            raise ValueError(f"'{config_key}'에 해당하는 설정을 'main_configs.json'에서 찾을 수 없습니다.")

        schema_path = os.path.join('schemas', config['schema_file'])
        schema_df = pd.read_csv(schema_path)
    except FileNotFoundError as e:
        raise ValueError(f"설정 또는 스키마 파일({e.filename})이 없습니다.")
    except Exception as e:
        raise e

    # 2. Google Sheets 인증 및 데이터 읽기
    credentials = get_google_credentials()
    gc = gspread.authorize(credentials)
    
    spreadsheet = gc.open_by_key(config['google_sheet_id'])
    worksheet = spreadsheet.worksheet(config['sheet_name'])
    
    print(f"[{config_key}] Google Sheet '{config['sheet_name']}'에서 데이터 읽기 시작 (범위: '{config['column_range']}')...", flush=True)
    
    # ✅ 변경: worksheet.get() → 재시도 함수로 교체
    data = get_sheet_data_with_retry(worksheet, config['column_range'], config_key)
     
    if len(data) < 2:
        print(f"[{config_key}] 시트에 데이터가 없거나 헤더만 존재합니다. 작업을 종료합니다.", flush=True)
        return

    print(f"[{config_key}] Google Sheet에서 {len(data) - 1}개의 행 데이터 읽기 완료.", flush=True)

    # 3. 데이터프레임 생성 및 컬럼 검증
    sheet_header = [h.strip() for h in data[0]]
    schema_header = schema_df['기존 컬럼명'].tolist()

    if sheet_header != schema_header:
        error_message = (
            f"컬럼명/순서 불일치 오류!\n"
            f"> Google Sheet 헤더: {sheet_header}\n"
            f"> 스키마 파일 헤더: {schema_header}"
        )
        print(f"[{config_key}] {error_message}", flush=True)
        raise ValueError(error_message)

    print(f"[{config_key}] 컬럼명 및 순서 검증 완료.", flush=True)

    expected_col_count = len(schema_header)
    normalized_data = []
    for i, row in enumerate(data[1:]):
        row_len = len(row)
        if row_len != expected_col_count:
            print(f"[{config_key}] 경고: 시트의 {i + 4}행 길이가 헤더({expected_col_count}개)와 다릅니다({row_len}개). 길이를 조정합니다.", flush=True)
            if row_len < expected_col_count:
                normalized_data.append(row + [''] * (expected_col_count - row_len))
            else:
                normalized_data.append(row[:expected_col_count])
        else:
            normalized_data.append(row)

    if not normalized_data:
        print(f"[{config_key}] 처리할 데이터가 없습니다. 작업을 종료합니다.", flush=True)
        return

    df = pd.DataFrame(normalized_data, columns=schema_df['영어 컬럼명'].tolist())
    print(f"[{config_key}] 데이터프레임 생성 완료. 총 {len(df)} 행.", flush=True)

    final_columns = schema_df['영어 컬럼명'].tolist()
    df = df[final_columns]

    # 4. 데이터 타입 변환
    print(f"[{config_key}] 데이터 타입 변환 중...", flush=True)
    for index, row in schema_df.iterrows():
        col_name = row['영어 컬럼명']
        col_type = row['데이터 타입'].upper()
        
        if col_name in df.columns:
            try:
                if col_type == 'STRING':
                    df[col_name] = df[col_name].astype(str).fillna('')
                elif col_type in ['INTEGER', 'INT64']:
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif col_type in ['FLOAT', 'FLOAT64']:
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
                elif col_type in ['BOOLEAN', 'BOOL']:
                    df[col_name] = df[col_name].str.lower().isin(['true', 't', '1']).astype(bool)
                elif col_type == 'DATE':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
                elif col_type == 'TIME':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.time
                elif col_type in ['DATETIME', 'TIMESTAMP']:
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            except Exception as e:
                print(f"[{config_key}] 경고: 컬럼 '{col_name}'({col_type}) 변환 오류: {e}", flush=True)
                df[col_name] = pd.NA

    print(f"[{config_key}] 데이터 타입 변환 완료.", flush=True)

    # 5. BigQuery 로드
    table_id = config['bigquery_table_id']
    project_id = table_id.split('.')[0]
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    
    bq_schema = [
        bigquery.SchemaField(row['영어 컬럼명'], row['데이터 타입'])
        for index, row in schema_df.iterrows()
    ]
    
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    print(f"[{config_key}] BigQuery '{table_id}' 로드 시작...", flush=True)
    print(f"[{config_key}] 데이터프레임 미리보기 (상위 5행):\n{df.head().to_string()}", flush=True)
    print(f"[{config_key}] 컬럼 타입:\n{df.dtypes.to_string()}", flush=True)

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"[{config_key}] 성공: {job.output_rows}개 행 로드 완료.", flush=True)
    return job.output_rows


@app.route('/process', methods=['POST'])
def process_sheet_request():
    payload = request.get_json()
    
    if not payload or 'config_key' not in payload:
        return Response("Bad Request: 요청 본문에 'config_key'가 필요합니다.", status=400)
        
    config_key = payload['config_key']
    table_name = ""
    try:
        with open('configs/main_configs.json', 'r', encoding='utf-8') as f:
            all_configs = json.load(f)
        config = all_configs.get(config_key)
        if config:
            table_name = config.get('bigquery_table_id', 'Unknown Table')

        print(f"[{config_key}] 작업 시작...", flush=True)
        num_rows_loaded = load_sheet_to_bigquery(config_key)
        send_slack_notification(config_key, table_name, True, num_rows=num_rows_loaded)
        send_jandi_notification(config_key, table_name, True, num_rows=num_rows_loaded)
        print(f"[{config_key}] 작업 성공적으로 완료.", flush=True)
        return Response(f"Success: Job for config_key '{config_key}' completed.", status=200)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"[{config_key}] 오류 발생: {error_details}", flush=True)
        send_slack_notification(config_key, table_name, False, error_message=str(e))
        send_jandi_notification(config_key, table_name, False, error_message=str(e))
        return Response(f"Internal Server Error:\n{error_details}", status=500)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
