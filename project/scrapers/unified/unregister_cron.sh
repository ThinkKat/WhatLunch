#!/usr/bin/env bash
set -euo pipefail

CRON_CMD="/opt/crawler/crawl_plan.sh >> /var/log/crawler/cron.log 2>&1"
( crontab -l 2>/dev/null | grep -v "$CRON_CMD" ) | crontab -

echo "[INFO] Removed cron for: $CRON_CMD"