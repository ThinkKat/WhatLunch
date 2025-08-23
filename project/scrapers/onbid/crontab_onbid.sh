#!/usr/bin/env bash
set -euo pipefail

# ==== 환경 보정 (크론 필수) ====
export PATH=/usr/local/bin:/usr/bin:/bin
DOCKER=/usr/bin/docker
AWS=/usr/bin/aws
WORKDIR="/home/ec2-user/crawlers/onbid"

# 날짜 변수 (YYYY-MM-DD 와 YYYYMMDD 둘 다 준비)
YESTERDAY=$(date -d "yesterday" +"%Y-%m-%d")      # 예: 2025-08-21
YESTERDAY_SHORT=$(date -d "yesterday" +"%Y%m%d")  # 예: 20250821

# 1. 작업 디렉토리 이동 (EC2에 scraper가 있는 곳)
cd "$WORKDIR"

# 2. Docker 실행 → 결과 파일 생성
"$DOCKER" run --rm -v "$WORKDIR:/app" --shm-size=1g onbid-scraper

# 3. 결과 파일 확인 (result/YYYY-MM-DD.csv)
RESULT_FILE="$WORKDIR/result/${YESTERDAY}.csv"
if [[ ! -f "$RESULT_FILE" ]]; then
  echo "[ERROR] daily onbid scraper에서 결과 파일이 생성되지 않았습니다: $RESULT_FILE"
  exit 1
fi

# 4. 업로드 대상 S3 경로
S3_PATH="s3://whatlunch-test/raw/onbid/${YESTERDAY_SHORT}/onbid-${YESTERDAY_SHORT}-raw.csv"

# 5. S3로 업로드
"$AWS" s3 cp "$RESULT_FILE" "$S3_PATH" --only-show-errors

echo "업로드 완료: $S3_PATH"