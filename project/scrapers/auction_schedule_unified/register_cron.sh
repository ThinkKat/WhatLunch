#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/opt/autoinside_schedule_crawler"
CRON_CMD="${BASE_DIR}/run_crawler.sh"
# 매일 새벽 3시 5분에 실행 (다른 크론 작업과 겹치지 않도록 시간 조절)
CRON_SCHEDULE="00 20 * * *"
CRON_ENTRY="$CRON_SCHEDULE $CRON_CMD"

# 기존에 등록된 동일한 명령어가 있다면 삭제하고 새로 추가합니다.
( crontab -l 2>/dev/null | grep -v "$CRON_CMD" ; \
  echo "SHELL=/bin/bash" ; \
  echo "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" ; \
  echo "$CRON_ENTRY" ) | crontab -

echo "[INFO] Autoinside 크롤러가 다음 스케줄로 등록되었습니다:"
echo "$CRON_ENTRY"
