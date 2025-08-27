# Playwright 공식 파이썬 이미지를 기반으로 합니다.
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

# 작업 디렉토리를 설정합니다.
WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul

# requirements.txt를 복사하고 라이브러리를 설치합니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

# 크롤러 소스 코드를 이미지 안으로 복사합니다.
COPY src/autoinside_auction_plan_ec2.py .

# 컨테이너가 시작될 때 크롤러를 바로 실행합니다.
ENTRYPOINT ["python", "-u", "autoinside_auction_plan_ec2.py"]