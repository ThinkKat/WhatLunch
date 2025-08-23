FROM mcr.microsoft.com/playwright/python:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 코드 복사
COPY crawl_base.py crawl_detail.py /app/
COPY daily_crawler.sh monthly_crawler.sh /app/

# 실행권한 부여
RUN chmod +x /app/daily_crawler.sh /app/monthly_crawler.sh 

RUN playwright install firefox
USER pwuser

# Entrypoint 지정 → 컨테이너 실행 시 자동으로 두 단계 실행됨
CMD ["/app/daily_crawler.sh"]