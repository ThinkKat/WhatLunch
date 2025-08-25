#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
json_to_csv_and_s3.py
Python 3.10 호환. 표준 csv 모듈로 JSON을 CSV로 변환한 뒤, boto3로 S3 업로드.

사용 예:
  python json_to_csv_and_s3.py \
    --file /mnt/data/complete_data_2025-08-22.json \
    --bucket whatlunch-test \
    --key raw/automart/2025-08-22/complete_data_2025-08-22.csv

옵션:
  --csv-out: 로컬 CSV 저장 경로(기본값: 입력 JSON 경로의 확장자만 .csv 로 변경)
  --encoding: CSV 파일 인코딩 (기본: utf-8-sig; 엑셀 호환)
  --delimiter: CSV 구분자 (기본: ,)
"""
import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Set

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert JSON to CSV and upload to S3.")
    p.add_argument("-f", "--file", required=True, help="입력 JSON 경로")
    p.add_argument("-b", "--bucket", required=True, help="업로드할 S3 버킷 이름")
    p.add_argument("-k", "--key", required=False, default=None, help="업로드할 S3 오브젝트 키 (예: path/to/file.csv)")
    p.add_argument("--csv-out", required=False, default=None, help="로컬 CSV 출력 경로")
    p.add_argument("--encoding", required=False, default="utf-8", help="CSV 인코딩 (기본: utf-8)")
    p.add_argument("--delimiter", required=False, default=",", help="CSV 구분자 (기본: ,)")
    return p.parse_args()

def load_rows(json_path: Path) -> List[Dict[str, Any]]:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # 가장 흔한 형태: 리스트[레코드...]
    if isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
        if not rows:
            raise ValueError("JSON 리스트 안에 dict 레코드가 없습니다.")
        return rows

    # dict인 경우: 값들이 전부 dict이면 값들을 레코드로 간주
    if isinstance(data, dict):
        vals = list(data.values())
        if vals and all(isinstance(v, dict) for v in vals):
            return vals
        # 흔한 케이스: {"data": [...]} 형태
        if "data" in data and isinstance(data["data"], list):
            rows = [r for r in data["data"] if isinstance(r, dict)]
            if rows:
                return rows

    raise ValueError("지원하지 않는 JSON 구조입니다. 리스트[dict...] 또는 dict(values=dict...) 또는 {'data':[dict...]} 형태가 필요합니다.")

def union_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
    # 첫 레코드의 키 순서를 우선으로 하고, 이후 새 키는 소팅하여 뒤에 붙임
    first_keys: List[str] = list(rows[0].keys())
    seen: Set[str] = set(first_keys)
    others: Set[str] = set()
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen:
                others.add(k)
    return first_keys + sorted(others)

def write_csv(csv_path: Path, rows: List[Dict[str, Any]], fieldnames: List[str], encoding: str, delimiter: str) -> None:
    with csv_path.open("w", newline="", encoding=encoding) as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", delimiter=delimiter)
        w.writeheader()
        for r in rows:
            # CSV에 문자열로 넣을 수 있게 None은 ""로 처리
            clean = {k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames}
            w.writerow(clean)

def upload_s3(csv_path: Path, bucket: str, key: str) -> None:
    import boto3  # pip install boto3
    s3 = boto3.client("s3")
    # ContentType 지정(선택)
    extra = {"ContentType": "text/csv; charset=utf-8"}
    s3.upload_file(str(csv_path), bucket, key, ExtraArgs=extra)

def main() -> int:
    args = parse_args()
    json_path = Path(args.file).expanduser().resolve()
    if not json_path.exists():
        print(f"[ERR] JSON 파일이 존재하지 않습니다: {json_path}", file=sys.stderr)
        return 1

    # CSV 로컬 경로 결정
    if args.csv_out:
        csv_path = Path(args.csv_out).expanduser().resolve()
    else:
        csv_path = json_path.with_suffix(".csv")

    try:
        rows = load_rows(json_path)
    except Exception as e:
        print(f"[ERR] JSON 로드/파싱 실패: {e}", file=sys.stderr)
        return 2

    fieldnames = union_fieldnames(rows)

    try:
        write_csv(csv_path, rows, fieldnames, args.encoding, args.delimiter)
    except Exception as e:
        print(f"[ERR] CSV 작성 실패: {e}", file=sys.stderr)
        return 3

    # S3 키 결정
    s3_key = args.key if args.key else csv_path.name

    try:
        upload_s3(csv_path, args.bucket, s3_key)
    except Exception as e:
        print(f"[ERR] S3 업로드 실패: {e}", file=sys.stderr)
        return 4

    print(f"[OK] CSV 저장: {csv_path}")
    print(f"[OK] S3 업로드: s3://{args.bucket}/{s3_key}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
