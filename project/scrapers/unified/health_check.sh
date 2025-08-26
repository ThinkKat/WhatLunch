sudo tee /opt/crawler/health_check.sh >/dev/null <<'SH'
#!/usr/bin/env bash
# usage: health_check.sh <service> [RUN_DATE:YYYY-MM-DD]
# RUN_DATE를 생략하면 오늘, 검사 대상 데이터일(DATA_DATE)은 RUN_DATE-1입니다.
set -euo pipefail

SVC="${1:?usage: health_check.sh <service> [RUN_DATE] }"
RUN_DATE="${2:-$(TZ=Asia/Seoul date +%F)}"

# DATA_DATE = RUN_DATE - 1 day
DATA_DATE="$(date -d "${RUN_DATE} -1 day" +%F)"
YMD="$(date -d "${DATA_DATE}" +%Y%m%d)"

LOG_ROOT="/var/log"
HEALTH_DIR="${LOG_ROOT}/crawler/health"
mkdir -p "$HEALTH_DIR"

status="fail"; location=""; bytes="-1"; note=""
aws_ok=0
command -v aws >/dev/null 2>&1 && aws_ok=1

case "$SVC" in
  autoinside)
    # S3: raw/autoinside/YYYY-MM-DD/autoinside-YYYYMMDD-raw.csv
    BUCKET="${BUCKET_AUTOINSIDE:-${BUCKET:-whatlunch-s3}}"
    KEY="raw/autoinside/${DATA_DATE}/autoinside-${YMD}-raw.csv"
    if (( aws_ok )) && size=$(aws s3api head-object --bucket "$BUCKET" --key "$KEY" --query 'ContentLength' --output text 2>/dev/null); then
      bytes="$size"; location="s3://${BUCKET}/${KEY}"
      if [ "$bytes" -gt 0 ]; then status="ok"; else note="zero_bytes"; fi
    else
      note="s3_head_object_failed_or_no_awscli"
    fi
    ;;

  autohub)
    # S3: raw/autohub/YYYY-MM-DD/autohub-YYYYMMDD-raw.csv
    BUCKET="${BUCKET_AUTOHUB:-whatlunch-s3}"
    KEY="raw/autohub/${DATA_DATE}/autohub-${YMD}-raw.csv"
    if (( aws_ok )) && size=$(aws s3api head-object --bucket "$BUCKET" --key "$KEY" --query 'ContentLength' --output text 2>/dev/null); then
      bytes="$size"; location="s3://${BUCKET}/${KEY}"
      if [ "$bytes" -gt 0 ]; then status="ok"; else note="zero_bytes"; fi
    else
      note="s3_head_object_failed_or_no_awscli"
    fi
    ;;

  onbid_daily)
    # Local: /data/onbid/result/onbid-YYYYMMDD-raw.csv
    CSV="/data/onbid/result/onbid-${YMD}-raw.csv"
    location="$CSV"
    if [ -s "$CSV" ]; then
      bytes=$(stat -c%s "$CSV" 2>/dev/null || echo -1)
      if [ "$bytes" -gt 0 ]; then status="ok"; else note="zero_bytes"; fi
    else
      note="file_missing_or_empty"
    fi
    ;;

  *)
    status="unknown"; note="unsupported_service"
    ;;
esac

# JSONL: run_date(배치 실행일)와 data_date(어제자) 모두 기록
printf '{"service":"%s","run_date":"%s","data_date":"%s","status":"%s","bytes":%s,"location":"%s","note":"%s"}\n' \
  "$SVC" "$RUN_DATE" "$DATA_DATE" "$status" "$bytes" "$location" "$note" \
  | tee -a "${HEALTH_DIR}/${RUN_DATE}.jsonl" >/dev/null
SH
sudo chmod +x /opt/crawler/health_check.sh
