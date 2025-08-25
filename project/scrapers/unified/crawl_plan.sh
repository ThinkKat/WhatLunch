#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="/opt/crawler/compose.yaml"
LOG_ROOT="/var/log"
CRON_LOG_DIR="${LOG_ROOT}/crawler"
AUTOINSIDE_LOG_DIR="${LOG_ROOT}/autoinside"
AUTOHUB_LOG_DIR="${LOG_ROOT}/autohub"
ONBID_LOG_DIR="${LOG_ROOT}/onbid"
AUTOMART_LOG_DIR="${LOG_ROOT}/automart" # automart 로그 디렉토리
LOCK_FILE="/var/run/crawler.lock"

DATE="$(TZ=Asia/Seoul date +%F)"
MAX_RETRY=2
BACKOFF_SEC=30

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
mkdir -p "$CRON_LOG_DIR" "$AUTOINSIDE_LOG_DIR" "$AUTOHUB_LOG_DIR" "$ONBID_LOG_DIR" "$AUTOMART_LOG_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] Another crawl is running. Exit."
  exit 0
fi

run_job() {
  local svc="$1"; local log_dir="$2"; local date_arg="$3"; local attempt=0
  local log_file="${log_dir}/nightly_${date_arg}.log"
  echo "[INFO] === Run ${svc} (DATE=${date_arg}) ===" | tee -a "$log_file"
  until docker compose -f "$COMPOSE_FILE" run --rm -e DATE="$date_arg" "$svc" >> "$log_file" 2>&1; do
    attempt=$((attempt+1))
    if (( attempt > MAX_RETRY )); then
      echo "[ERROR] ${svc} failed after $((MAX_RETRY+1)) attempts." | tee -a "$log_file"; return 1
    fi
    echo "[WARN] ${svc} failed (attempt ${attempt}/${MAX_RETRY}). Backoff ${BACKOFF_SEC}s..." | tee -a "$log_file"
    sleep "$BACKOFF_SEC"
  done
  echo "[INFO] ${svc} done." | tee -a "$log_file"
}

run_job "autoinside" "$AUTOINSIDE_LOG_DIR" "$DATE"
run_job "autohub"    "$AUTOHUB_LOG_DIR"    "$DATE"
run_job "onbid_daily" "$ONBID_LOG_DIR"    "$DATE"
run_job "automart"   "$AUTOMART_LOG_DIR"   "$DATE" # automart 작업 실행 추가

echo "[INFO] All jobs done for ${DATE}." | tee -a "${CRON_LOG_DIR}/cron_${DATE}.log"
