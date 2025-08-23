#!/bin/bash
set -euo pipefail

mkdir -p /app/base /app/result

# 오늘 ~ +30일
FROM_DATE=$(date +"%Y-%m-%d")
TO_DATE=$(date -d "$FROM_DATE +30 days" +"%Y-%m-%d")

BASE_OUT="/app/base/new_${FROM_DATE}_${TO_DATE}.json"
DETAIL_OUT="/app/result/new_${FROM_DATE}_${TO_DATE}.csv"

echo "[INFO] Running crawl_base.py (NEW)... $FROM_DATE ~ $TO_DATE"
python /app/crawl_base.py \
  --mode NEW \
  --from "$FROM_DATE" \
  --to "$TO_DATE" \
  --categories 12101 12102 12103 \
  --out "$BASE_OUT" \
  --max-pages 500

echo "[INFO] Running crawl_detail.py (NEW)... input=$BASE_OUT"
python /app/crawl_detail.py \
  --mode NEW \
  --input "$BASE_OUT" \
  --out "$DETAIL_OUT"

echo "[OK] Done -> $DETAIL_OUT"
