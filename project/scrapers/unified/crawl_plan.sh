#!/usr/bin/env bash
# resilient crawl_plan.sh — 한 크롤러가 실패해도 나머지는 계속 수행
set -euo pipefail

COMPOSE_FILE="/opt/crawler/compose.yaml"
LOG_ROOT="/var/log"
DATE="$(TZ=Asia/Seoul date +%F)"     # 컨테이너에 DATE 전달
MAX_RETRY=2
BACKOFF_SEC=30

# 실행 PATH (cron 환경 보강)
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# 동시 실행 방지용 락
LOCK_FILE="/var/run/crawler.lock"
mkdir -p "$(dirname "$LOCK_FILE")" "$LOG_ROOT/crawler"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[WARN] Another crawl is running. Exit."
  exit 0
fi

# 실행할 서비스 목록 (필요 시 SERVICES env로 덮어쓰기 가능: SERVICES="onbid_daily autohub")
DEFAULT_SERVICES=("autohub" "autoinside" "onbid_daily" "automart")
read -r -a SERVICES <<< "${SERVICES:-${DEFAULT_SERVICES[*]}}"

# 서비스별 로그 디렉토리 매핑
log_dir_for() {
  case "$1" in
    onbid_daily) echo "${LOG_ROOT}/onbid" ;;
    autohub)     echo "${LOG_ROOT}/autohub" ;;
    autoinside)  echo "${LOG_ROOT}/autoinside" ;;
    automart)    echo "${LOG_ROOT}/automart" ;;
    *)           echo "${LOG_ROOT}/$1" ;;
  esac
}

run_job() {
  local svc="$1"
  local log_dir="$2"
  local date_arg="$3"
  local attempt=0
  local log_file="${log_dir}/nightly_${date_arg}.log"

  mkdir -p "$log_dir"

  echo "[INFO] === Run ${svc} (DATE=${date_arg}) ===" | tee -a "$log_file"

  # 실패해도 전체 스크립트가 죽지 않도록, 이 함수는 항상 자체적으로 상태를 반환
  set +e
  until docker compose -f "$COMPOSE_FILE" run --rm -e DATE="$date_arg" "$svc" >> "$log_file" 2>&1; do
    attempt=$((attempt+1))
    if (( attempt > MAX_RETRY )); then
      echo "[ERROR] ${svc} failed after $((MAX_RETRY+1)) attempts." | tee -a "$log_file"
      set -e
      return 1
    fi
    echo "[WARN] ${svc} failed (attempt ${attempt}/${MAX_RETRY}). Backoff ${BACKOFF_SEC}s..." | tee -a "$log_file"
    sleep "$BACKOFF_SEC"
  done
  set -e

  echo "[INFO] ${svc} done." | tee -a "$log_file"
  return 0
}

# 실행 루프: 실패해도 계속
declare -a SUCCEEDED=()
declare -a FAILED=()

for svc in "${SERVICES[@]}"; do
  log_dir="$(log_dir_for "$svc")"
  if run_job "$svc" "$log_dir" "$DATE"; then
    SUCCEEDED+=("$svc")
  else
    FAILED+=("$svc")
  fi
done

# 요약
echo "----------------------------------------" | tee -a "${LOG_ROOT}/crawler/cron_${DATE}.log"
echo "[INFO] Completed all jobs for ${DATE}" | tee -a "${LOG_ROOT}/crawler/cron_${DATE}.log"
echo "[INFO] Succeeded: ${SUCCEEDED[*]:-<none>}" | tee -a "${LOG_ROOT}/crawler/cron_${DATE}.log"
echo "[INFO] Failed   : ${FAILED[*]:-<none>}"   | tee -a "${LOG_ROOT}/crawler/cron_${DATE}.log"

# 하나라도 실패했으면 비정상 종료코드로 종료(크론 알림/모니터링용)
if (( ${#FAILED[@]} > 0 )); then
  exit 1
fi
exit 0
