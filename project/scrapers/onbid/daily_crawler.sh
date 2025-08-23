#!/bin/bash
set -euo pipefail

mkdir -p /app/base /app/result

FROM_DATE=$(date -d "yesterday" +"%Y-%m-%d")
TO_DATE=$(date -d "yesterday" +"%Y-%m-%d")

BASE_OUT="/app/base/${TO_DATE}.json"
DETAIL_OUT="/app/result/${TO_DATE}.csv"

echo "[INFO] Running crawl_base.py... $FROM_DATE ~ $TO_DATE"
python /app/crawl_base.py \
  --mode HISTORY \
  --from "$FROM_DATE" \
  --to "$TO_DATE" \
  --categories 12101 12102 12103 \
  --out "$BASE_OUT" \
  --max-pages 500

echo "[INFO] Running crawl_detail.py... input=$BASE_OUT"
python /app/crawl_detail.py \
  --mode HISTORY \
  --input "$BASE_OUT" \
  --out "$DETAIL_OUT"

echo "[OK] Done -> $DETAIL_OUT"
