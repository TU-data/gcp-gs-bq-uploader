import os
import json
import pandas as pd
import gspread
from flask import Flask, request, Response
from google.auth import default
from google.cloud import bigquery
from slack_notifier import send_slack_notification
from jandi_notifier import send_jandi_notification

app = Flask(__name__)

def get_google_credentials():
    """
    Cloud Run 환경에 최적화된 방식으로 Google 서비스 자격 증명을 가져옵니다.
    로컬 테스트 시에는 GOOGLE_APPLICATION_CREDENTIALS 환경변수를 설정해야 합니다.
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/bigquery'
    ]
    credentials, project = default(scopes=scopes)
    return credentials

def load_sheet_to_bigquery(config_key: str):
    """지정된 설정을 기반으로 Google Sheet 데이터를 BigQuery에 로드합니다."""

    # 1. 설정 파일 로드 및 키 확인
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

    # 2. Google Sheets API 인증 및 데이터 읽기
    credentials = get_google_credentials()
    gc = gspread.authorize(credentials)
    
    spreadsheet = gc.open_by_key(config['google_sheet_id'])
    worksheet = spreadsheet.worksheet(config['sheet_name'])
    
    print(f"[{config_key}] Google Sheet '{config['sheet_name']}'에서 데이터 읽기 시작 (범위: '{config['column_range']}')...", flush=True)
    data = worksheet.get(config['column_range'])
     
    if len(data) < 2:
        print(f"[{config_key}] 시트에 데이터가 없거나 헤더만 존재합니다. 작업을 종료합니다.", flush=True)
        return

    print(f"[{config_key}] Google Sheet에서 {len(data) - 1}개의 행 데이터 읽기 완료.", flush=True)

    # 3. Pandas 데이터프레임 생성 및 스키마 기반 컬럼명 할당
    # Google Sheet의 헤더와 스키마 파일의 컬럼명 순서 및 내용 일치 여부 검증
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

    print(f"[{config_key}] 컬럼명 및 순서 검증 완료: Google Sheet와 스키마 파일의 헤더가 일치합니다.", flush=True)

    # 데이터 정규화: 모든 데이터 행의 길이를 헤더/스키마의 컬럼 수에 맞게 조정
    expected_col_count = len(schema_header)
    normalized_data = []
    # data[0]은 헤더, data[1:]은 실제 데이터. column_range가 'B3:AS'이므로 실제 시트 행은 i + 4
    for i, row in enumerate(data[1:]):
        row_len = len(row)
        if row_len != expected_col_count:
            print(f"[{config_key}] 경고: 시트의 {i + 4}행 길이가 헤더({expected_col_count}개)와 다릅니다({row_len}개). 길이를 조정합니다.", flush=True)
            if row_len < expected_col_count:
                # 행이 짧으면 빈 문자열로 채움
                normalized_data.append(row + [''] * (expected_col_count - row_len))
            else: # row_len > expected_col_count
                # 행이 길면 자름
                normalized_data.append(row[:expected_col_count])
        else:
            normalized_data.append(row)

    # 데이터가 없는 경우 처리
    if not normalized_data:
        print(f"[{config_key}] 처리할 데이터가 없습니다. 작업을 종료합니다.", flush=True)
        return

    # 정규화된 데이터로 데이터프레임 생성 후, 스키마의 영문 컬럼명을 직접 할당
    df = pd.DataFrame(normalized_data, columns=schema_df['영어 컬럼명'].tolist())
    
    print(f"[{config_key}] 데이터프레임 생성 및 영문 컬럼명 할당 완료. 총 {len(df)} 행.", flush=True)

    # 최종 컬럼은 스키마에 정의된 영문 컬럼 전체 (순서 고정)
    final_columns = schema_df['영어 컬럼명'].tolist()
    df = df[final_columns]
    
    print(f"[{config_key}] 컬럼명 매핑 및 순서 고정 완료. 최종 컬럼: {df.columns.tolist()}", flush=True)

    # 4. 스키마에 따라 데이터 타입 변환 (안정성 강화)
    print(f"[{config_key}] 스키마에 따라 데이터 타입을 변환합니다...", flush=True)
    for index, row in schema_df.iterrows():
        col_name = row['영어 컬럼명']
        col_type = row['데이터 타입'].upper()
        
        if col_name in df.columns:
            try:
                if col_type == 'STRING':
                    df[col_name] = df[col_name].astype(str).fillna('')
                elif col_type in ['INTEGER', 'INT64']:
                    # 쉼표 제거 및 숫자로 변환할 수 없는 값은 0으로 대체
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif col_type in ['FLOAT', 'FLOAT64']:
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
                elif col_type in ['BOOLEAN', 'BOOL']:
                    # 'true', 't', '1' (대소문자 무관) 외에는 모두 False로 처리
                    df[col_name] = df[col_name].str.lower().isin(['true', 't', '1']).astype(bool)
                elif col_type == 'DATE':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
                elif col_type == 'TIME':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.time
                elif col_type in ['DATETIME', 'TIMESTAMP']:
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            except Exception as e:
                print(f"[{config_key}] 경고: 컬럼 '{col_name}'({col_type}) 변환 중 오류 발생. 데이터를 건너뜁니다. 오류: {e}", flush=True)
                # 오류 발생 시 해당 컬럼을 null 값으로 채울 수도 있음
                df[col_name] = pd.NA

    print(f"[{config_key}] 데이터 타입 변환 완료.", flush=True)

    # 5. BigQuery에 데이터 로드
    # bigquery_table_id는 'project.dataset.table' 전체 문자열이어야 함
    table_id = config['bigquery_table_id']
    # project는 table_id에서 추출
    project_id = table_id.split('.')[0]
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    
    # BigQuery 스키마 정의
    bq_schema = [
        bigquery.SchemaField(row['영어 컬럼명'], row['데이터 타입'])
        for index, row in schema_df.iterrows()
    ]
    
    # 데이터프레임을 BigQuery 테이블에 로드
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition="WRITE_TRUNCATE",  # 테이블이 있으면 덮어쓰기
        create_disposition="CREATE_IF_NEEDED" # 테이블이 없으면 스키마에 따라 생성
    )

    # BQ 클라이언트를 사용하여 데이터프레임에서 직접 로드
    print(f"[{config_key}] BigQuery 테이블 '{table_id}'에 데이터 로드 시작 (스키마: {bq_schema})...", flush=True)
    print(f"[{config_key}] 데이터프레임 미리보기 (상위 5행):\n{df.head().to_string()}", flush=True)
    print(f"[{config_key}] 데이터프레임 컬럼 타입:\n{df.dtypes.to_string()}", flush=True)

    job = bigquery_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()  # 작업이 완료될 때까지 대기

    print(f"[{config_key}] 성공: {job.output_rows}개의 행을 BigQuery 테이블 {table_id}에 로드했습니다.", flush=True)
    return job.output_rows


@app.route('/process', methods=['POST'])
def process_sheet_request():
    """Cloud Scheduler로부터 POST 요청을 받아 데이터 처리를 시작하는 엔드포인트"""
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
        # 에러의 전체 트레이스백을 문자열로 캡처합니다.
        error_details = traceback.format_exc()
        
        # Cloud Run 로그에 상세 에러를 출력합니다.
        print(f"[{config_key}] 오류 발생: {error_details}", flush=True)
        send_slack_notification(config_key, table_name, False, error_message=str(e))
        send_jandi_notification(config_key, table_name, False, error_message=str(e))

        # HTTP 응답 본문에 상세 에러를 포함하여 디버깅을 돕습니다.
        return Response(f"Internal Server Error:\n{error_details}", status=500)

if __name__ == "__main__":
    # Cloud Run 환경에서는 PORT 환경변수를 사용
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
