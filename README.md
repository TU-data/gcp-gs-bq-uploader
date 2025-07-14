# Google Sheets to BigQuery Pipeline

## 1. 개요

이 프로젝트는 Google Sheets에 저장된 데이터를 읽어와 지정된 스키마에 따라 데이터를 변환한 후, Google BigQuery 테이블에 적재하는 자동화 파이프라인입니다.

Flask 기반의 웹 애플리케이션으로 구현되었으며, Docker를 사용하여 컨테이너화되고 Google Cloud Build를 통해 Google Cloud Run에 자동으로 배포됩니다. Cloud Scheduler와 같은 스케줄링 도구를 사용하여 주기적으로 데이터 파이프라인을 실행할 수 있습니다.

## 2. 주요 기능

-   **설정 기반 실행**: `configs` 폴더의 JSON 설정 파일을 통해 여러 종류의 데이터 파이프라인을 관리할 수 있습니다.
-   **스키마 관리**: `schemas` 폴더의 CSV 파일을 사용하여 BigQuery 테이블의 스키마와 Google Sheets의 컬럼명을 매핑하고 데이터 타입을 정의합니다.
-   **데이터 변환**: Pandas를 사용하여 데이터를 읽고, 정의된 스키마에 따라 컬럼명을 변경하고 데이터 타입을 변환합니다.
-   **BigQuery 적재**: BigQuery의 `WRITE_TRUNCATE` 옵션을 사용하여 매번 실행 시 테이블의 모든 데이터를 삭제하고 새로 적재합니다.
-   **컨테이너화 및 서버리스**: Dockerfile을 통해 실행 환경을 패키징하고, Cloud Run을 통해 서버 관리 없이 파이프라인을 실행합니다.
-   **CI/CD 자동화**: `cloudbuild.yaml` 파일을 통해 소스 코드가 변경될 때마다 자동으로 빌드 및 Cloud Run에 배포됩니다.

## 3. 프로젝트 구조

```
.
├── cloudbuild.yaml       # Google Cloud Build 설정 파일 (CI/CD)
├── Dockerfile            # 애플리케이션 컨테이너 이미지 정의
├── main.py               # 핵심 로직이 담긴 Flask 애플리케이션
├── requirements.txt      # Python 패키지 의존성 목록
├── configs/
│   └── call_criteria.json  # 데이터 파이프라인 설정 예시 (JSON)
└── schemas/
    └── call_criteria.csv   # BigQuery 테이블 스키마 및 컬럼 매핑 예시 (CSV)
```

-   `main.py`: HTTP POST 요청을 수신하여 데이터 처리 작업을 트리거하는 Flask 애플리케이션입니다.
-   `Dockerfile`: Python 3.11 환경에서 애플리케이션을 실행하기 위한 Docker 이미지 설정 파일입니다.
-   `cloudbuild.yaml`: 코드를 빌드하고 Cloud Run에 배포하는 CI/CD 파이프라인 정의 파일입니다.
-   `configs/`: 각 파이프라인의 설정을 정의하는 JSON 파일이 위치합니다. (Google Sheet ID, BigQuery 테이블 ID 등)
-   `schemas/`: BigQuery 테이블의 스키마(컬럼명, 데이터 타입)를 정의하는 CSV 파일이 위치합니다.

## 4. 동작 방식

1.  외부 서비스(예: Google Cloud Scheduler)가 Cloud Run 서비스의 `/process` 엔드포인트로 HTTP POST 요청을 보냅니다. 요청 본문(body)에는 실행할 설정 파일의 이름(`config_name`)이 포함되어야 합니다.
2.  `main.py`의 Flask 애플리케이션이 요청을 받아 `config_name`에 해당하는 `configs/{config_name}.json` 파일을 읽습니다.
3.  설정 파일에 명시된 Google Sheet ID와 시트 이름, 범위를 사용하여 `gspread` 라이브러리로 데이터를 조회합니다.
4.  `schemas/{schema_file}.csv` 파일을 읽어 컬럼명 매핑 정보와 데이터 타입을 가져옵니다.
5.  Pandas를 사용하여 조회한 데이터의 컬럼명을 변경하고, 정의된 데이터 타입으로 변환합니다.
6.  Google BigQuery 클라이언트를 사용하여 변환된 Pandas DataFrame을 목표 테이블에 직접 적재합니다. 이때 `WRITE_TRUNCATE` 옵션을 사용하여 기존 데이터를 모두 삭제하고 새로 입력합니다.

## 5. 설치 및 배포

### 사전 준비 사항

-   Google Cloud Platform 프로젝트
-   활성화된 API:
    -   Google Cloud Build API
    -   Google Cloud Run API
    -   Google BigQuery API
    -   Google Sheets API
-   Cloud Run 서비스를 실행하고 BigQuery 및 Google Sheets에 접근할 권한을 가진 서비스 계정(Service Account)

### 설정 방법

1.  **파이프라인 설정 추가**:
    -   `configs` 디렉토리에 `my_pipeline.json` 과 같은 새로운 설정 파일을 추가합니다.
    -   `schemas` 디렉토리에 `my_schema.csv` 와 같이 위 설정에 맞는 스키마 파일을 추가합니다.
2.  **Cloud Build 설정**:
    -   `cloudbuild.yaml` 파일에서 `PROJECT_ID`, Cloud Run 서비스 이름, 서비스 계정 등을 환경에 맞게 수정합니다.

### 배포

-   로컬에 `gcloud` CLI가 설치된 경우, 아래 명령어를 사용하여 수동으로 빌드 및 배포를 트리거할 수 있습니다.

    ```bash
    gcloud builds submit --config cloudbuild.yaml .
    ```

-   또는, Cloud Source Repositories와 같은 Git 저장소와 Cloud Build 트리거를 연결하여 코드 푸시 시 자동으로 배포되도록 설정할 수 있습니다.

## 6. 사용 방법

Cloud Run에 서비스가 성공적으로 배포되면, 아래와 같이 HTTP POST 요청을 보내 파이프라인을 실행할 수 있습니다.

`{SERVICE_URL}`은 Cloud Run 배포 후 생성된 서비스의 URL입니다.

```bash
curl -X POST {SERVICE_URL}/process \
-H "Content-Type: application/json" \
-d '{ \
  "config_name": "call_criteria" \
}'
```

-   `config_name`: `configs` 디렉토리에 있는 설정 파일의 이름 (확장자 `.json` 제외)을 지정합니다.
-   이 요청은 Cloud Scheduler에 등록하여 원하는 시간마다 주기적으로 실행하도록 설정할 수 있습니다.
