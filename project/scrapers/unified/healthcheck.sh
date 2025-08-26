#!/usr/bin/env bash
set -euo pipefail

# 기본값: 버킷/사이트/날짜는 환경변수나 인수로 바꿀 수 있음
BUCKET="${BUCKET:-whatlunch-s3}"
SITES="${SITES:-onbid,autohub,autoinside,automart}"
DATE_ARG="${1:-}"   # 인수로 YYYY-MM-DD 넘기면 그 날짜 체크, 없으면 KST 어제

CMD=(python3 /opt/crawler/healthcheck_crawlers.py --bucket "$BUCKET" --sites "$SITES")
if [[ -n "$DATE_ARG" ]]; then
  CMD+=("--date" "$DATE_ARG")
fi

"${CMD[@]}"
