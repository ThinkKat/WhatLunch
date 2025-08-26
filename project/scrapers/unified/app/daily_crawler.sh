#!/bin/bash
set -euo pipefail

APP_DIR="/app"
S3_BUCKET="whatlunch-s3"          # 필요시 환경변수로 주면: S3_BUCKET=${S3_BUCKET:-whatlunch-s3}
S3_PREFIX_RAW="raw/onbid"

# 어제 날짜 (KST)
TARGET_DATE=$(TZ=Asia/Seoul date -d "yesterday" +"%Y-%m-%d")
TARGET_DATE_NODASH=$(TZ=Asia/Seoul date -d "yesterday" +"%Y%m%d")

LOG_DIR="${APP_DIR}/logs"
LOG_FILE="${LOG_DIR}/crawl_${TARGET_DATE_NODASH}.log"

BASE_OUT_FILE="${APP_DIR}/base/${TARGET_DATE}.json"
FINAL_CSV_DIR="${APP_DIR}/result/onbid/${TARGET_DATE}"
FINAL_CSV_FILE="${FINAL_CSV_DIR}/onbid-${TARGET_DATE_NODASH}-raw.csv"

mkdir -p "${APP_DIR}/base" "$FINAL_CSV_DIR" "$LOG_DIR"

export PYTHONUNBUFFERED=1
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[INFO] Starting onbid (Date=${TARGET_DATE})"
echo "[INFO] Dependencies are assumed preinstalled in the image."

# === base 수집 ===
python "${APP_DIR}/onbid/crawl_base.py" \
  --mode HISTORY \
  --from "$TARGET_DATE" \
  --to "$TARGET_DATE" \
  --categories 12101 12102 12103 \
  --out "$BASE_OUT_FILE" \
  --max-pages 500

if [ ! -s "$BASE_OUT_FILE" ]; then
  echo "[ERROR] Base JSON missing or empty: $BASE_OUT_FILE"
  exit 1
fi
echo "[OK] Base JSON -> $BASE_OUT_FILE"

# === detail 수집 ===
python "${APP_DIR}/onbid/crawl_detail.py" \
  --mode HISTORY \
  --input "$BASE_OUT_FILE" \
  --out "$FINAL_CSV_FILE" \
  --workers 4 \
  --retries 3 \
  --log-file "$LOG_FILE"

if [ -s "$FINAL_CSV_FILE" ]; then
  echo "[OK] Detail CSV -> ${FINAL_CSV_FILE}"
else
  echo "[WARN] Detail finished but no data; CSV not created or empty."
fi

# === S3 업로드 (boto3 사용: aws CLI 불필요) ===
#   CSV  : s3://$S3_BUCKET/raw/onbid/YYYY-MM-DD/onbid-YYYYMMDD-raw.csv
#   로그 : s3://$S3_BUCKET/logs/onbid/YYYY-MM-DD/crawl_YYYYMMDD.log
python - <<'PY' "$FINAL_CSV_FILE" "$LOG_FILE" "$S3_BUCKET" "$TARGET_DATE" "$TARGET_DATE_NODASH"
import sys, os
csv_path, log_path, bucket, date_folder, date_nodash = sys.argv[1:6]

# boto3 필요
try:
    import boto3
except Exception as e:
    print(f"[ERROR] boto3 is not installed: {e}")
    raise SystemExit(2)

s3 = boto3.client("s3")
site = "onbid"

def upload_if_exists(local_path: str, key: str):
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        s3.upload_file(local_path, bucket, key)
        print(f"[OK] Uploaded s3://{bucket}/{key}")
    else:
        print(f"[WARN] Skip upload (missing/empty): {local_path}")

# CSV 업로드 (있을 때만)
csv_key = f"raw/{site}/{date_folder}/onbid-{date_nodash}-raw.csv"
upload_if_exists(csv_path, csv_key)

# 로그 업로드
log_key = f"logs/{site}/{date_folder}/crawl_{date_nodash}.log"
upload_if_exists(log_path, log_key)
PY

echo "[INFO] onbid_daily done (CSV + log uploaded via boto3)."
