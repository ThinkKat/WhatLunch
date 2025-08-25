#!/bin/bash
set -euo pipefail

# --- 설정 ---
APP_DIR="/app"
S3_BUCKET="whatlunch-s3"
S3_BASE_PATH="onbid"

# --- 날짜 및 경로 변수 설정 ---
TARGET_DATE=$(date -d "yesterday" +"%Y-%m-%d")
TARGET_DATE_NODASH=$(date -d "yesterday" +"%Y%m%d")

# 로그 파일 경로
LOG_DIR="${APP_DIR}/logs"
LOG_FILE="${LOG_DIR}/crawl_${TARGET_DATE_NODASH}.log"

# 데이터 저장 경로
BASE_OUT_FILE="${APP_DIR}/base/${TARGET_DATE}.json"
FINAL_CSV_DIR="${APP_DIR}/result/${S3_BASE_PATH}/${TARGET_DATE}"
FINAL_CSV_FILE="${FINAL_CSV_DIR}/onbid-${TARGET_DATE_NODASH}-raw.csv"

# --- 디렉토리 생성 ---
mkdir -p "${APP_DIR}/base" "$FINAL_CSV_DIR" "$LOG_DIR"

# --- 환경 설정 및 의존성 설치 ---
echo "[INFO] Setting up environment..."
if [ -f "${APP_DIR}/requirements.txt" ]; then
    pip install -r "${APP_DIR}/requirements.txt"
fi
playwright install firefox

# --- 스크립트 실행 ---
echo "[INFO] Running crawl_base.py... Date: ${TARGET_DATE}"
python "${APP_DIR}/onbid/crawl_base.py" \
  --mode HISTORY \
  --from "$TARGET_DATE" \
  --to "$TARGET_DATE" \
  --categories 12101 12102 12103 \
  --out "$BASE_OUT_FILE" \
  --max-pages 500

if [ ! -f "$BASE_OUT_FILE" ]; then
    echo "[ERROR] Base JSON file not found: $BASE_OUT_FILE" | tee -a "$LOG_FILE"
    exit 1
fi

echo "[INFO] Running crawl_detail.py... Input: ${BASE_OUT_FILE}"
python "${APP_DIR}/onbid/crawl_detail.py" \
  --mode HISTORY \
  --input "$BASE_OUT_FILE" \
  --out "$FINAL_CSV_FILE" \
  --workers 4 \
  --retries 3 \
  --log-file "$LOG_FILE"

# --- 결과 확인 및 S3 업로드 ---
if [ -f "$FINAL_CSV_FILE" ]; then
    echo "[OK] Crawling successful, data found -> ${FINAL_CSV_FILE}"
    
    # S3로 결과 및 로그 파일 업로드 (필요시 주석 해제)
    # S3_TARGET_PATH="s3://${S3_BUCKET}/${S3_BASE_PATH}/${TARGET_DATE}/"
    # echo "[INFO] Uploading result to S3 -> ${S3_TARGET_PATH}"
    # aws s3 cp "${FINAL_CSV_FILE}" "${S3_TARGET_PATH}"
    # aws s3 cp "${LOG_FILE}" "${S3_TARGET_PATH}"
else
    echo "[OK] Crawling successful, no data for today. Skipping file creation and upload."
    # 데이터가 없더라도 로그 파일은 S3로 업로드 할 수 있습니다 (필요시 주석 해제)
    # S3_TARGET_PATH="s3://${S3_BUCKET}/${S3_BASE_PATH}/${TARGET_DATE}/"
    # echo "[INFO] Uploading log file to S3 -> ${S3_TARGET_PATH}"
    # aws s3 cp "${LOG_FILE}" "${S3_TARGET_PATH}"
fi

echo "[INFO] onbid_daily done."
