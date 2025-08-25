FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy
WORKDIR /app

# 필수 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 브라우저 설치
RUN playwright install chromium firefox

# 앱 소스 복사
COPY run.py ./
COPY daily_crawler.sh ./
COPY sites/ ./sites/
COPY onbid/ ./onbid/

# 실행 권한
RUN chmod +x /app/daily_crawler.sh

# 로그/데이터 디렉토리
RUN mkdir -p /logs /data /app/base /app/result
ENV TZ=Asia/Seoul

ENTRYPOINT ["python", "-u", "run.py"]
