# Python 3.11 버전을 베이스 이미지로 사용
FROM python:3.11-slim

# 환경변수 설정
ENV PYTHONUNBUFFERED True

# 작업 디렉토리 설정
WORKDIR /app

# requirements.txt를 먼저 복사하고 패키지를 설치 (레이어 캐싱 활용)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 나머지 소스코드 복사
COPY . .

# gunicorn 웹 서버로 애플리케이션 실행
# PORT 환경변수는 Cloud Run에서 자동으로 주입
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]