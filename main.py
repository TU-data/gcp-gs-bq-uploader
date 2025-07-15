import os
import json
import pandas as pd
import gspread
from flask import Flask, request, Response
from google.auth import default
from google.cloud import bigquery

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

    # 3. Pandas 데이터프레임 생성 및 컬럼명 변경 
    # 첫 행은 헤더, 나머지는 데이터
    # Google Sheet의 컬럼명 앞뒤 공백 제거
    header = [h.strip() for h in data[0]]
    df = pd.DataFrame(data[1:], columns=header)

    print(f"[{config_key}] 데이터프레임 생성 완료. 총 {len(df)} 행.", flush=True)

    # -- 디버깅 로그 추가 시작 --
    print(f"[{config_key}] Google Sheet에서 읽어온 컬럼: {list(df.columns)}", flush=True)
    print(f"[{config_key}] 스키마 파일에 정의된 컬럼: {list(schema_df['기존 컬럼명'])}", flush=True)

    sheet_columns = set(df.columns)
    schema_columns = set(schema_df['기존 컬럼명'])
    
    missing_in_sheet = schema_columns - sheet_columns
    missing_in_schema = sheet_columns - schema_columns
    
    if missing_in_sheet or missing_in_schema:
        error_message = "컬럼명 불일치 오류!\n"
        if missing_in_sheet:
            error_message += f"> Google Sheet에 다음 컬럼이 없습니다: {list(missing_in_sheet)}\n"
        if missing_in_schema:
            error_message += f"> 스키마 파일(schemas/{config['schema_file']})에 다음 컬럼이 없습니다: {list(missing_in_schema)}\n"
        print(f"[{config_key}] {error_message}", flush=True)
        raise ValueError(error_message)
    
    print(f"[{config_key}] 컬럼명 검증 완료: Google Sheet와 스키마 파일의 컬럼명이 일치합니다.", flush=True)
    # -- 디버깅 로그 추가 끝 --
    
    # 스키마 파일 기반으로 컬럼명 매핑 (한글 -> 영문)
    column_mapping = dict(zip(schema_df['기존 컬럼명'], schema_df['영어 컬럼명']))
    df.rename(columns=column_mapping, inplace=True)
    
    # 정의된 영문 컬럼만 선택하여 순서 고정
    final_columns = [col for col in column_mapping.values() if col in df.columns]
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
                    # 숫자로 변환할 수 없는 값은 0으로 대체
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif col_type in ['FLOAT', 'FLOAT64']:
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
                elif col_type == 'BOOLEAN':
                    # 'true', 't', '1' (대소문자 무관) 외에는 모두 False로 처리
                    df[col_name] = df[col_name].str.lower().isin(['true', 't', '1']).astype(bool)
                # DATE, DATETIME, TIMESTAMP 등 다른 타입 추가 가능
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


@app.route('/process', methods=['POST'])
def process_sheet_request():
    """Cloud Scheduler로부터 POST 요청을 받아 데이터 처리를 시작하는 엔드포인트"""
    payload = request.get_json()
    
    if not payload or 'config_key' not in payload:
        return Response("Bad Request: 요청 본문에 'config_key'가 필요합니다.", status=400)
        
    config_key = payload['config_key']
    
    try:
        print(f"[{config_key}] 작업 시작...", flush=True)
        print(f"[{config_key}] 작업 성공적으로 완료.", flush=True)
        return Response(f"Success: Job for config_key '{config_key}' completed.", status=200)
    except Exception as e:
        import traceback
        # 에러의 전체 트레이스백을 문자열로 캡처합니다.
        error_details = traceback.format_exc()
        
        # Cloud Run 로그에 상세 에러를 출력합니다.
        print(f"[{config_key}] 오류 발생: {error_details}", flush=True)

        # HTTP 응답 본문에 상세 에러를 포함하여 디버깅을 돕습니다.
        return Response(f"Internal Server Error:\n{error_details}", status=500)

if __name__ == "__main__":
    # Cloud Run 환경에서는 PORT 환경변수를 사용
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
