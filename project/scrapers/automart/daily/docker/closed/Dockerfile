# Base: Playwright + Python + Ubuntu 22.04 (Jammy)
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV TZ=Asia/Seoul \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    DEBIAN_FRONTEND=noninteractive

# 앱/브라우저 경로 준비 및 유저 생성
RUN apt update && \
    apt install -y --no-install-recommends \
        python3-venv \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m thinkcat && \
    mkdir -p /app /ms-playwright && \
    chown -R thinkcat:thinkcat /app /ms-playwright

# 일반 유저로 전환
USER thinkcat
WORKDIR /app

RUN python3 -m venv .venv && \
    .venv/bin/pip install bs4 playwright && \
    .venv/bin/playwright install chromium

COPY --chown=thinkcat:thinkcat . /app/automart/.






