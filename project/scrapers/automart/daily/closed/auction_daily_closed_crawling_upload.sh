#!/usr/bin/env bash
# run_crawling_and_upload.sh
# set -x  # 디버그 원하면 주석 해제
set -euo pipefail

# 기본값
URL_DEFAULT="https://www.automart.co.kr/views/pub_auction/pub_auction_intro.asp?num=4"
DATE=""                     # 미지정 시 어제(Asia/Seoul)
timeout_sec=300             # 기본 300, 쉘에서 사용하지 않고 크롤러에만 전달
BUCKET="whatlunch-s3"       # 미지정 시 기본 버킷
FILE=""                     # 미지정 시 complete_data_{DATE}.json
KEY=""                      # 미지정 시 raw/automart/{YYYY-MM-DD}/automart_{YYYYMMDD}_raw.csv

print_usage() {
  cat <<EOF
사용법:
  $(basename "$0") [옵션]

옵션 (crawling):
  --url URL                 (기본: ${URL_DEFAULT})
  --date YYYY-MM-DD         (미지정 시 어제, Asia/Seoul 기준)
  --timeout_sec N           (기본: 300)  # 셸에서 타임아웃을 걸지 않으며, 크롤러에만 전달

옵션 (upload):
  --file PATH               (기본: automart/daily/closed/complete_data_{\$date}.json)
  --bucket NAME             (기본: whatlunch-s3)
  --key S3_KEY              (기본: raw/automart/{YYYY-MM-DD}/automart_{YYYYMMDD}_raw.csv)

예시:
  $(basename "$0") --date 2025-08-22
  $(basename "$0") --url "https://..." --timeout_sec 600 --bucket my-bucket
EOF
}

# --- 인자 파싱 ---
URL="$URL_DEFAULT"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) URL="$2"; shift 2;;
    --date) DATE="$2"; shift 2;;
    --timeout_sec) timeout_sec="$2"; shift 2;;
    --file) FILE="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --key) KEY="$2"; shift 2;;
    -h|--help) print_usage; exit 0;;
    *) echo "[ERR] 알 수 없는 옵션: $1"; print_usage; exit 1;;
  esac
done

# --- 날짜 기본값: 어제(Asia/Seoul) ---
if [[ -z "${DATE}" ]]; then
  if command -v gdate >/dev/null 2>&1; then
    DATE="$(TZ=Asia/Seoul gdate -d 'yesterday' +%F)"
  else
    DATE="$(TZ=Asia/Seoul date -d 'yesterday' +%F)"
  fi
fi
DATE_NODASH="${DATE//-/}"

# --- 기본 파일/키 구성 ---
if [[ -z "${FILE}" ]]; then
  FILE="automart/daily/closed/complete_data_${DATE}.json"
  # 현재 디렉토리에 없으면 /mnt/data 도 시도
  if [[ ! -f "${FILE}" && -f "/mnt/data/${FILE}" ]]; then
    FILE="/mnt/data/${FILE}"
  fi
fi

if [[ -z "${KEY}" ]]; then
  KEY="raw/automart/${DATE}/automart_${DATE_NODASH}_raw.csv"
fi

# --- 스크립트/도구 경로 ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY=${PYTHON:-python3}

CRAWL_PY="${SCRIPT_DIR}/auction_daily_closed_crawling.py"
[[ -f "${CRAWL_PY}" ]] || CRAWL_PY="auction_daily_closed_crawling.py"

JSON2CSV_S3_PY="${SCRIPT_DIR}/auction_daily_closed_upload.py"
[[ -f "${JSON2CSV_S3_PY}" ]] || JSON2CSV_S3_PY="auction_daily_closed_upload.py"

# --- 사전 체크 ---
command -v "${PY}" >/dev/null 2>&1 || { echo "[ERR] python 실행 파일을 찾지 못했습니다(PYTHON=${PY})."; exit 2; }
[[ -f "${CRAWL_PY}" ]] || { echo "[ERR] 크롤링 스크립트를 찾지 못했습니다: ${CRAWL_PY}"; exit 2; }
[[ -f "${JSON2CSV_S3_PY}" ]] || { echo "[ERR] JSON→CSV→S3 업로드 스크립트를 찾지 못했습니다: ${JSON2CSV_S3_PY}"; exit 2; }

# --- 크롤링 실행 (timeout_sec은 크롤러에만 전달; 셸에서 타임아웃 사용 안 함) ---
echo "[INFO] Crawling 시작"
echo "       URL=${URL}"
echo "       DATE=${DATE}"
echo "       timeout_sec=${timeout_sec}"
set +e
"${PY}" "${CRAWL_PY}" --url "${URL}" --date "${DATE}" --timeout_sec "${timeout_sec}"
rc=$?
set -e
# if [[ $rc -ne 0 ]]; then
#   echo "[WARN] 인자 포함 실행 실패(rc=${rc}), 인자 없이 재시도"
#   "${PY}" "${CRAWL_PY}" || { echo "[ERR] 크롤링 실패"; exit 3; }
# fi

# --- 결과 파일 확인 ---
if [[ ! -f "${FILE}" ]]; then
  echo "[ERR] 결과 JSON 파일을 찾지 못했습니다: ${FILE}"
  exit 4
fi

# --- JSON → CSV 변환 + S3 업로드 ---
echo "[INFO] 업로드 준비"
echo "       FILE=${FILE}"
echo "       BUCKET=${BUCKET}"
echo "       KEY=${KEY}"
"${PY}" "${JSON2CSV_S3_PY}" \
  --file "${FILE}" \
  --bucket "${BUCKET}" \
  --key "${KEY}"

echo "[OK] 완료: s3://${BUCKET}/${KEY}"
