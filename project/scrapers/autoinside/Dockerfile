FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV TZ=Asia/Seoul
WORKDIR /app

# 1) 프로젝트 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Playwright 파이썬 패키지 + 브라우저 확실히 설치
RUN pip install --no-cache-dir "playwright==1.47.*" \
 && playwright install chromium

# 3) 앱 코드
COPY autoinside_daily_ec2.py .

ENTRYPOINT ["python", "autoinside_daily_ec2.py"]
