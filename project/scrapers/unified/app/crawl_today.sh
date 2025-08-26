#!/bin/bash
set -euo pipefail

# --- 설정 ---
# 파이썬 스크립트가 있는 디렉토리 (현재 위치에 onbid 폴더가 있다고 가정)
# 만약 스크립트들이 현재 위치에 바로 있다면 SCRIPT_DIR="." 으로 변경
SCRIPT_DIR="./onbid" 

# --- 날짜 및 경로 변수 설정 ---
# 오늘 날짜를 기준으로 설정
TARGET_DATE=$(date +"%Y-%m-%d")
TARGET_DATE_NODASH=$(date +"%Y%m%d")

# 결과를 저장할 로컬 디렉토리 (현재 위치 기준)
BASE_DIR="./base"
RESULT_DIR="./result"

# 데이터 저장 경로
BASE_OUT_FILE="${BASE_DIR}/${TARGET_DATE}.json"
FINAL_CSV_FILE="${RESULT_DIR}/onbid-${TARGET_DATE_NODASH}-raw.csv"

# --- 디렉토리 생성 ---
mkdir -p "$BASE_DIR" "$RESULT_DIR"

# --- 환경 설정 및 의존성 설치 ---
echo "[INFO] Setting up local environment..."
# requirements.txt 파일이 있다면 의존성 설치
if [ -f "./requirements.txt" ]; then
    pip install -r "./requirements.txt"
fi
# Playwright 브라우저 설치 (firefox)
playwright install firefox

# --- 스크립트 실행 ---
echo "[INFO] Running crawl_base.py for date: ${TARGET_DATE}"
python "${SCRIPT_DIR}/crawl_base.py" \
  --mode HISTORY \
  --from "$TARGET_DATE" \
  --to "$TARGET_DATE" \
  --categories 12101 12102 12103 \
  --out "$BASE_OUT_FILE" \
  --max-pages 500

if [ ! -f "$BASE_OUT_FILE" ]; then
    echo "[ERROR] Base JSON file was not created: $BASE_OUT_FILE"
    exit 1
fi

echo "[INFO] Running crawl_detail.py with input: ${BASE_OUT_FILE}"
python "${SCRIPT_DIR}/crawl_detail.py" \
  --mode HISTORY \
  --input "$BASE_OUT_FILE" \
  --out "$FINAL_CSV_FILE" \
  --workers 4 \
  --retries 3 \
  --log-file "./crawl_local_${TARGET_DATE_NODASH}.log"

# --- 결과 확인 ---
if [ -f "$FINAL_CSV_FILE" ]; then
    echo "[OK] Successfully created -> ${FINAL_CSV_FILE}"
else
    echo "[ERROR] Final CSV file was not created."
    exit 1
fi
