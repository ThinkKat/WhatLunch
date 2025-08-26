sudo tee /opt/crawler/health_all.sh >/dev/null <<'SH'
#!/usr/bin/env bash
# usage: health_all.sh [YYYY-MM-DD]
set -euo pipefail
DATE="${1:-$(TZ=Asia/Seoul date +%F)}"
SVC_LIST="${SVC_LIST:-autoinside autohub onbid_daily}"
for s in $SVC_LIST; do
  /opt/crawler/health_check.sh "$s" "$DATE"
done

FILE="/var/log/crawler/health/${DATE}.jsonl"
echo "----- HEALTH SUMMARY ${DATE} -----"
if command -v jq >/dev/null 2>&1; then
  jq -r '.service+"  "+.status+"  "+.details' "$FILE"
else
  cat "$FILE"
fi
SH
sudo chmod +x /opt/crawler/health_all.sh
