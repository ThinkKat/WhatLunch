FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy
WORKDIR /app

# 필수 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- noninteractive 모드 추가 ---
# DEBIAN_FRONTEND=noninteractive를 설정하여 빌드가 멈추는 현상을 방지합니다.
ENV TZ=Asia/Seoul
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# ---------------------------------------------

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

ENTRYPOINT ["python", "-u", "run.py"]