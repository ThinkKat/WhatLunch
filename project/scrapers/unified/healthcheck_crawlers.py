#!/usr/bin/env python3
"""
S3 기반 크롤러 헬스체크
- 기본 버킷: whatlunch-s3
- 기본 점검 대상: onbid, autohub, autoinside, automart
- 기본 날짜: KST 기준 어제
- 성공 판정: CSV 객체 존재 && 크기 >= 200바이트 && (헤더 포함 2줄 이상)
           + 로그 객체 존재 && 크기 >= 1바이트
반환 코드:
  0 = 모두 통과
  1 = 일부 실패
  2 = 설정/자격증명 등 치명적 오류
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

KST = timezone(timedelta(hours=9))


def kst_yesterday():
    d = datetime.now(KST) - timedelta(days=1)
    return d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d")


def expected_keys(site: str, date_folder: str, date_nodash: str):
    site = site.lower()
    # CSV 경로
    if site == "onbid":
        csv_key = f"raw/onbid/{date_folder}/onbid-{date_nodash}-raw.csv"
        log_key = f"logs/onbid/{date_folder}/crawl_{date_nodash}.log"
    else:
        csv_key = f"raw/{site}/{date_folder}/{site}-{date_nodash}-raw.csv"
        log_key = f"logs/{site}/{date_folder}/crawl_{date_nodash}.log"
    return csv_key, log_key


def head_ok(s3, bucket, key, min_bytes=1):
    try:
        rsp = s3.head_object(Bucket=bucket, Key=key)
        size = rsp.get("ContentLength", 0)
        return (size >= min_bytes, size, None)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        return (False, 0, code or str(e))


def csv_has_2lines(s3, bucket, key, max_bytes=1024 * 1024):
    # 헤더+데이터 최소 2줄 판단(최대 1MB만 읽음)
    try:
        rsp = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{max_bytes-1}")
        body = rsp["Body"].read()
        try:
            text = body.decode("utf-8-sig", errors="ignore")
        except Exception:
            text = body.decode("utf-8", errors="ignore")
        # 개행으로 세어보되, 마지막 줄 끝 개행 없을 수 있음
        lines = [ln for ln in text.strip().splitlines() if ln.strip() != ""]
        return (len(lines) >= 2, len(lines))
    except ClientError as e:
        return (False, 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.environ.get("BUCKET", "whatlunch-s3"))
    parser.add_argument(
        "--sites",
        default=os.environ.get("SITES", "onbid,autohub,autoinside,automart"),
        help="쉼표 구분 소문자 사이트명 리스트",
    )
    parser.add_argument("--date", help="YYYY-MM-DD (기본: KST 어제)")
    parser.add_argument("--csv-min-bytes", type=int, default=200)
    parser.add_argument("--log-min-bytes", type=int, default=1)
    args = parser.parse_args()

    if args.date:
        try:
            date_folder = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y-%m-%d")
            date_nodash = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y%m%d")
        except Exception:
            print(f"[ERROR] --date 형식이 잘못됐습니다: {args.date} (예: 2025-08-25)")
            return 2
    else:
        date_folder, date_nodash = kst_yesterday()

    sites = [s.strip().lower() for s in args.sites.split(",") if s.strip()]
    s3 = boto3.client("s3")

    print(f"[INFO] Healthcheck bucket={args.bucket}, date={date_folder}, sites={sites}")
    any_fail = False
    results = []

    for site in sites:
        csv_key, log_key = expected_keys(site, date_folder, date_nodash)

        csv_exists, csv_size, csv_err = head_ok(
            s3, args.bucket, csv_key, args.csv_min_bytes
        )
        log_exists, log_size, log_err = head_ok(
            s3, args.bucket, log_key, args.log_min_bytes
        )

        csv_lines_ok, line_count = (False, 0)
        if csv_exists:
            csv_lines_ok, line_count = csv_has_2lines(s3, args.bucket, csv_key)

        site_ok = csv_exists and csv_lines_ok and log_exists
        any_fail |= not site_ok

        results.append(
            {
                "site": site,
                "csv_key": csv_key,
                "csv_size": csv_size,
                "csv_min": args.csv_min_bytes,
                "csv_lines_ok": csv_lines_ok,
                "csv_line_count": line_count,
                "csv_error": csv_err,
                "log_key": log_key,
                "log_size": log_size,
                "log_min": args.log_min_bytes,
                "log_error": log_err,
                "ok": site_ok,
            }
        )

    print("\n=== RESULTS ===")
    for r in results:
        status = "PASS" if r["ok"] else "FAIL"
        print(f"[{status}] {r['site']}")
        print(
            f"  CSV : s3://{args.bucket}/{r['csv_key']}  size={r['csv_size']}  >= {r['csv_min']}  lines>=2? {r['csv_lines_ok']} (count={r['csv_line_count']})"
        )
        if r["csv_error"]:
            print(f"        error={r['csv_error']}")
        print(
            f"  LOG : s3://{args.bucket}/{r['log_key']}  size={r['log_size']}  >= {r['log_min']}"
        )
        if r["log_error"]:
            print(f"        error={r['log_error']}")

    return 0 if not any_fail else 1


if __name__ == "__main__":
    sys.exit(main())
