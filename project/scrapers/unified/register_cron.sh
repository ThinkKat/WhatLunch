#!/usr/bin/env bash
set -euo pipefail

CRON_CMD="/opt/crawler/crawl_plan.sh >> /var/log/crawler/cron.log 2>&1"
CRON_SCHEDULE="0 2 * * *"   # 매일 02:00
CRON_ENTRY="$CRON_SCHEDULE $CRON_CMD"

( crontab -l 2>/dev/null | grep -v "$CRON_CMD" ; \
  echo "SHELL=/bin/bash" ; \
  echo "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin" ; \
  echo "$CRON_ENTRY" ) | crontab -

echo "[INFO] Installed cron:"; echo "$CRON_ENTRY"