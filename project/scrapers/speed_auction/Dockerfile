# --- Base: Playwright Python (브라우저/런타임 모두 포함) ---
# 버전은 적절히 고정하세요 (예: v1.47.x). 최신 사용 시 태그만 바꾸면 됩니다.
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

# 로케일/타임존(로그/파일 타임스탬프 일관성)
ENV TZ=Asia/Seoul \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 시스템 패키지 (선택): lxml 가속용 libxml2/xslt, tzdata 등
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    tzdata \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

# 워킹 디렉토리
WORKDIR /app

# 파이썬 패키지 설치
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt || \
    pip install --no-cache-dir \
        requests \
        pandas \
        beautifulsoup4 \
        lxml \
        tqdm

# 애플리케이션 복사
COPY . /app

# 결과 저장 폴더 미리 생성
RUN mkdir -p /app/result

# 컨테이너 실행 시 tqdm/로그가 실시간으로 보이도록 -u
ENTRYPOINT ["python", "-u", "crawl.py"]

# 기본 값: --help (docker run만 쳤을 때 도움말)
CMD ["--help"]

