#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/opt/autoinside_crawler"
CRON_CMD="${BASE_DIR}/run_crawler.sh"

# Crontab에서 해당 명령어를 찾아 삭제합니다.
( crontab -l 2>/dev/null | grep -v "$CRON_CMD" ) | crontab -

echo "[INFO] Autoinside 크롤러의 자동 실행 스케줄이 삭제되었습니다."
