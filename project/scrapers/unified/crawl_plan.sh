sudo tee /opt/crawler/crawl_plan.sh >/dev/null <<'SH'
#!/usr/bin/env bash
# /opt/crawler/crawl_plan.sh
set -euo pipefail
STREAM_CONSOLE=${STREAM_CONSOLE:-1}   # 1: 콘솔+파일 동시 출력, 0: 파일만

PROJECT_DIR="/opt/crawler"
cd "$PROJECT_DIR"

# Compose 자동 감지
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "[FATAL] Docker Compose not installed." >&2; exit 127
fi

# compose 파일 확인 (CWD)
if [[ -f compose.yaml ]]; then
  : # ok
elif [[ -f docker-compose.yml ]]; then
  ln -sf docker-compose.yml compose.yaml
else
  echo "[FATAL] compose.yaml/docker-compose.yml not found in $PWD" >&2; exit 66
fi

LOG_ROOT="/var/log"
CRON_LOG_DIR="${LOG_ROOT}/crawler"
AUTOINSIDE_LOG_DIR="${LOG_ROOT}/autoinside"
AUTOHUB_LOG_DIR="${LOG_ROOT}/autohub"
ONBID_LOG_DIR="${LOG_ROOT}/onbid"
LOCK_FILE="/var/run/crawler.lock"

DATE="$(TZ=Asia/Seoul date +%F)"
MAX_RETRY=2
BACKOFF_SEC=30

mkdir -p "$CRON_LOG_DIR" "$AUTOINSIDE_LOG_DIR" "$AUTOHUB_LOG_DIR" "$ONBID_LOG_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then echo "[WARN] Another crawl is running. Exit."; exit 0; fi

run_job() {
  local svc="$1" log_dir="$2" date_arg="$3" attempt=0
  local log_file="${log_dir}/nightly_${date_arg}.log"
  echo "[INFO] === Run ${svc} (DATE=${date_arg}) ===" | tee -a "$log_file"

  while true; do
    if (( STREAM_CONSOLE )); then
      if "${COMPOSE[@]}" run --rm -e DATE="$date_arg" "$svc" 2>&1 | tee -a "$log_file"; then
        break
      fi
    else
      if "${COMPOSE[@]}" run --rm -e DATE="$date_arg" "$svc" >> "$log_file" 2>&1; then
        break
      fi
    fi
    attempt=$((attempt+1))
    if (( attempt > MAX_RETRY )); then
      echo "[ERROR] ${svc} failed after $((MAX_RETRY+1)) attempts." | tee -a "$log_file"
      return 1
    fi
    echo "[WARN] ${svc} failed (attempt ${attempt}/${MAX_RETRY}). Backoff ${BACKOFF_SEC}s..." | tee -a "$log_file"
    sleep "$BACKOFF_SEC"
  done
  echo "[INFO] ${svc} done." | tee -a "$log_file"
}

# 실행 + 헬스 기록
run_job "autoinside" "$AUTOINSIDE_LOG_DIR" "$DATE" && /opt/crawler/health_check.sh autoinside "$DATE"
run_job "autohub"    "$AUTOHUB_LOG_DIR"    "$DATE" && /opt/crawler/health_check.sh autohub "$DATE"
run_job "onbid_daily" "$ONBID_LOG_DIR"    "$DATE" && /opt/crawler/health_check.sh onbid_daily "$DATE"

# 요약/알림
/opt/crawler/health_all.sh "$DATE" || true
if [[ -n "${SLACK_WEBHOOK_URL:-}" ]] && [[ -x /opt/crawler/health_notify.sh ]]; then
  /opt/crawler/health_notify.sh "$DATE" || true
fi

echo "[INFO] All jobs done for ${DATE}." | tee -a "${CRON_LOG_DIR}/cron_${DATE}.log"
SH
sudo chmod +x /opt/crawler/crawl_plan.sh
