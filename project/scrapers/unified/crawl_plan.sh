sudo tee /opt/crawler/crawl_plan.sh >/dev/null <<'SH'
#!/usr/bin/env bash
<<<<<<< HEAD
# resilient crawl_plan.sh — 한 크롤러가 실패해도 나머지는 계속 수행
=======
# /opt/crawler/crawl_plan.sh
>>>>>>> cf5005b (feat(health-check): 헬스체크 추가.)
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
<<<<<<< HEAD
DATE="$(TZ=Asia/Seoul date +%F)"     # 컨테이너에 DATE 전달
MAX_RETRY=2
BACKOFF_SEC=30

# 실행 PATH (cron 환경 보강)
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
=======
CRON_LOG_DIR="${LOG_ROOT}/crawler"
AUTOINSIDE_LOG_DIR="${LOG_ROOT}/autoinside"
AUTOHUB_LOG_DIR="${LOG_ROOT}/autohub"
ONBID_LOG_DIR="${LOG_ROOT}/onbid"
LOCK_FILE="/var/run/crawler.lock"

DATE="$(TZ=Asia/Seoul date +%F)"
MAX_RETRY=2
BACKOFF_SEC=30

mkdir -p "$CRON_LOG_DIR" "$AUTOINSIDE_LOG_DIR" "$AUTOHUB_LOG_DIR" "$ONBID_LOG_DIR"
>>>>>>> cf5005b (feat(health-check): 헬스체크 추가.)

# 동시 실행 방지용 락
LOCK_FILE="/var/run/crawler.lock"
mkdir -p "$(dirname "$LOCK_FILE")" "$LOG_ROOT/crawler"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then echo "[WARN] Another crawl is running. Exit."; exit 0; fi

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
<<<<<<< HEAD
  local svc="$1"
  local log_dir="$2"
  local date_arg="$3"
  local attempt=0
=======
  local svc="$1" log_dir="$2" date_arg="$3" attempt=0
>>>>>>> cf5005b (feat(health-check): 헬스체크 추가.)
  local log_file="${log_dir}/nightly_${date_arg}.log"

  mkdir -p "$log_dir"

  echo "[INFO] === Run ${svc} (DATE=${date_arg}) ===" | tee -a "$log_file"

<<<<<<< HEAD
  # 실패해도 전체 스크립트가 죽지 않도록, 이 함수는 항상 자체적으로 상태를 반환
  set +e
  until docker compose -f "$COMPOSE_FILE" run --rm -e DATE="$date_arg" "$svc" >> "$log_file" 2>&1; do
    attempt=$((attempt+1))
    if (( attempt > MAX_RETRY )); then
      echo "[ERROR] ${svc} failed after $((MAX_RETRY+1)) attempts." | tee -a "$log_file"
      set -e
=======
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
>>>>>>> cf5005b (feat(health-check): 헬스체크 추가.)
      return 1
    fi
    echo "[WARN] ${svc} failed (attempt ${attempt}/${MAX_RETRY}). Backoff ${BACKOFF_SEC}s..." | tee -a "$log_file"
    sleep "$BACKOFF_SEC"
  done
  set -e

  echo "[INFO] ${svc} done." | tee -a "$log_file"
  return 0
}

<<<<<<< HEAD
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
=======
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
>>>>>>> cf5005b (feat(health-check): 헬스체크 추가.)
