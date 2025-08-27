#!/usr/bin/env bash
set -euo pipefail

CRON_CMD="/opt/crawler/crawl_plan.sh >> /var/log/crawler/cron.log 2>&1"

# 화~토 한국시간 02:00
CRON_SCHEDULE="0 2 * * 2-6"
CRON_ENTRY="CRON_TZ=Asia/Seoul
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
${CRON_SCHEDULE} ${CRON_CMD}
"

TMP="$(mktemp)"
{ crontab -l 2>/dev/null || true; } | awk -v cmd="$CRON_CMD" 'index($0, cmd)==0' > "$TMP"
printf "%s" "$CRON_ENTRY" >> "$TMP"
crontab "$TMP"
rm -f "$TMP"
crontab -l || true