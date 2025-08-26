#!/usr/bin/env bash
set -euo pipefail

CRON_CMD="/opt/auction_crawler/run_crawler.sh"

# 대상 사용자 (기본: 현재 사용자). 예) TARGET_USER=root ./unregister_autoinside_schedule_tue_sat.sh
TARGET_USER="${TARGET_USER:-}"

if [[ -n "${TARGET_USER}" ]]; then
  CRONTAB_LIST=(crontab -u "${TARGET_USER}" -l)
  CRONTAB_APPLY=(crontab -u "${TARGET_USER}")
else
  CRONTAB_LIST=(crontab -l)
  CRONTAB_APPLY=(crontab)
fi

TMP_IN="$(mktemp)"
TMP_OUT="$(mktemp)"
{ "${CRONTAB_LIST[@]}" 2>/dev/null || true; } > "${TMP_IN}"
awk -v cmd="$CRON_CMD" 'index($0,cmd)==0' "${TMP_IN}" > "${TMP_OUT}"
"${CRONTAB_APPLY[@]}" "${TMP_OUT}"
rm -f "${TMP_IN}" "${TMP_OUT}"
