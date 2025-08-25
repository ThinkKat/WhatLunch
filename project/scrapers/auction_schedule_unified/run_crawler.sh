#!/usr/bin/env bash
set -euo pipefail

# 스크립트가 위치한 디렉토리를 기준으로 경로를 설정합니다.
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
COMPOSE_FILE="${BASE_DIR}/compose.yaml"

# 로그 파일 및 잠금 파일 설정
LOG_DIR="/var/log/autoinside_schedule_crawler"
LOCK_FILE="/var/run/autoinside_schedule_crawler.lock"
DATE=$(TZ=Asia/Seoul date +%F)
LOG_FILE="${LOG_DIR}/crawl_${DATE}.log"

# 로그 디렉토리가 없으면 생성합니다.
mkdir -p "$LOG_DIR"

# flock을 사용하여 스크립트가 동시에 여러 번 실행되는 것을 방지합니다.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] $(date): 이전 크롤링 작업이 아직 실행 중입니다. 이번 작업을 건너뜁니다." >> "$LOG_FILE"
  exit 0
fi

echo "[INFO] $(date): Autoinside 크롤링을 시작합니다." | tee -a "$LOG_FILE"

# docker compose를 사용하여 크롤러를 실행하고, 모든 출력을 로그 파일에 기록합니다.
if docker compose -f "$COMPOSE_FILE" run --rm autoinside-crawler >> "$LOG_FILE" 2>&1; then
  echo "[INFO] $(date): 크롤링 작업이 성공적으로 완료되었습니다." | tee -a "$LOG_FILE"
else
  echo "[ERROR] $(date): 크롤링 작업 중 오류가 발생했습니다. 자세한 내용은 로그 파일을 확인하세요." | tee -a "$LOG_FILE"
fi
