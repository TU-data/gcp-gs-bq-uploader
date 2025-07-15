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
│   └── main_configs.json  # 여러 파이프라인 설정을 포함하는 JSON 파일
└── schemas/
    └── call_criteria.csv   # BigQuery 테이블 스키마 및 컬럼 매핑 예시 (CSV)
```

-   `main.py`: HTTP POST 요청을 수신하여 데이터 처리 작업을 트리거하는 Flask 애플리케이션입니다.
-   `Dockerfile`: Python 3.11 환경에서 애플리케이션을 실행하기 위한 Docker 이미지 설정 파일입니다.
-   `cloudbuild.yaml`: 코드를 빌드하고 Cloud Run에 배포하는 CI/CD 파이프라인 정의 파일입니다.
-   `configs/`: 각 파이프라인의 설정을 정의하는 JSON 파일이 위치합니다. (Google Sheet ID, BigQuery 테이블 ID 등)
-   `schemas/`: BigQuery 테이블의 스키마(컬럼명, 데이터 타입)를 정의하는 CSV 파일이 위치합니다.

## 4. 동작 방식

1.  외부 서비스(예: Google Cloud Scheduler)가 Cloud Run 서비스의 `/process` 엔드포인트로 HTTP POST 요청을 보냅니다. 요청 본문(body)에는 실행할 설정의 키(`config_key`)가 포함되어야 합니다.
2.  `main.py`의 Flask 애플리케이션이 요청을 받아 `config_key`에 해당하는 `configs/main_configs.json` 파일에서 설정을 읽습니다.
3.  설정 파일에 명시된 Google Sheet ID와 시트 이름, 범위를 사용하여 `gspread` 라이브러리로 데이터를 조회합니다.
4.  `schemas/{schema_file}.csv` 파일을 읽어 컬럼명 매핑 정보와 데이터 타입을 가져옵니다.
5.  Pandas를 사용하여 조회한 데이터의 컬럼명을 변경하고, 정의된 데이터 타입으로 변환합니다.
6.  Google BigQuery 클라이언트를 사용하여 변환된 Pandas DataFrame을 목표 테이블에 직접 적재합니다. 이때 `WRITE_TRUNCATE` 옵션을 사용하여 기존 데이터를 모두 삭제하고 새로 입력합니다.
7.  모든 과정은 상세한 한글 로그와 함께 `Cloud Run 로그`에 기록되어 진행 상황을 쉽게 파악할 수 있습니다.

## 5. 설치 및 배포 (Google Cloud Platform UI)

### 사전 준비 사항

-   **Google Cloud Platform 프로젝트**: 프로젝트가 생성되어 있고, 결제가 활성화되어 있어야 합니다.
-   **소스 코드 저장소**: GitHub 또는 Cloud Source Repositories에 프로젝트 코드가 푸시되어 있어야 합니다.
-   **활성화된 API**:
    -   Google Cloud Build API
    -   Google Cloud Run API
    -   Google BigQuery API
    -   Google Sheets API
-   **서비스 계정 (Service Account)**: Cloud Run 서비스를 실행하고 BigQuery 및 Google Sheets에 접근할 수 있는 권한(예: `BigQuery 데이터 편집자`, `Google Sheets API에 대한 접근 권한`)을 가진 서비스 계정이 필요합니다.

### 파이프라인 설정

-   `configs/main_configs.json` 파일에 파이프라인 설정을 추가하거나 수정합니다. 각 설정은 Google Sheet ID, BigQuery 테이블 ID 등의 정보를 포함해야 합니다.
-   `schemas` 디렉토리에 각 파이프라인에 맞는 스키마 CSV 파일을 추가합니다.

### 배포

이 프로젝트는 CI/CD 파이프라인이 설정되어 있어, Git 저장소의 특정 브랜치(예: `main`)에 코드를 푸시하면 자동으로 빌드 및 배포가 진행됩니다.

1.  **소스 코드 변경**: 로컬에서 코드를 수정한 후 Git에 커밋합니다.
2.  **Git Push**: 원격 저장소의 해당 브랜치로 푸시합니다.
    ```bash
    git push origin main
    ```
3.  **자동 배포**: 푸시된 코드는 Cloud Build 트리거에 의해 감지되어, `cloudbuild.yaml` 설정에 따라 자동으로 애플리케이션을 빌드하고 Cloud Run에 새로운 버전으로 배포합니다.

## 6. 사용 방법 (Cloud Scheduler로 자동 실행 설정)

배포된 Cloud Run 서비스를 주기적으로 실행하여 데이터 파이프라인을 자동화하려면 Google Cloud Scheduler를 사용합니다.

1.  **Cloud Scheduler 페이지로 이동**:
    -   Google Cloud Console에서 'Cloud Scheduler'로 이동합니다.

2.  **작업 만들기 (Create Job)**:
    -   '작업 만들기'를 클릭하여 새 스케줄러 작업을 생성합니다.
    -   기존 작업을 복사 하는 것이 편리합니다. 

3.  **작업 정보 입력**:
    -   **이름**: 작업의 고유한 이름을 입력합니다 (예: `daily-data-pipeline`).
    -   **리전**: Cloud Run 서비스가 배포된 리전과 동일한 리전을 선택합니다.
    -   **빈도**: 원하는 실행 주기를 [cron](https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules) 형식으로 입력합니다 (예: 매일 오전 9시에 실행하려면 `0 9 * * *`).
    -   **시간대**: 기준 시간대를 선택합니다 (예: `Asia/Seoul`).

4.  **실행 대상 구성**:
    -   **대상 유형**: `HTTP`를 선택합니다.
    -   **URL**: 배포된 Cloud Run 서비스의 URL에 `/process` 엔드포인트를 추가하여 입력합니다. (예: `https://<your-cloud-run-service-url>/process`)
    -   **HTTP 메서드**: `POST`를 선택합니다.
    -   **본문**: 실행할 파이프라인의 `config_key`를 JSON 형식으로 입력합니다.
        ```json
        {
          "config_key": "daily-data-pipeline"
        }
        ```
        (여기서 `"01"`은 `configs/main_configs.json`에 정의된 키입니다.)

5.  **인증 설정**:
    -   **HTTP 헤더** 섹션 아래에서 **인증 헤더 추가**를 클릭합니다.
    -   **인증 유형**: `OIDC 토큰`을 선택합니다.
    -   **서비스 계정**: Cloud Scheduler가 Cloud Run 서비스를 호출할 때 사용할 서비스 계정을 선택합니다. 이 서비스 계정에는 `Cloud Run 호출자(roles/run.invoker)` 역할이 부여되어 있어야 합니다.

6.  **만들기**:
    -   '만들기'를 클릭하여 스케줄러 작업을 저장하고 활성화합니다.

이제 Cloud Scheduler가 설정된 빈도에 따라 자동으로 Cloud Run 서비스를 호출하여 데이터 파이프라인을 실행합니다.

