# -*- coding: utf-8 -*-
"""
AWS Lambda: 차량명으로 '브랜드', '모델명', '차종' 생성 + 파일명 규칙 변환 (Pandas 미사용)

입력 S3 Object Key 형식:  [경매장]-[YYYYMMDD]-raw.csv
출력 S3 Object Key 형식:  [경매장]-[YYYYMMDD]-normal_name.csv

동작 개요
1) CSV의 차량 이름 컬럼을 자동 탐색하거나, 이미 존재하는 브랜드/회사명/모델명 컬럼을 활용해 정규화합니다.
2) brand_to_fuzzy.json, brand_norm_to_fuzzy.json(브랜드별 모델 별칭)로 브랜드/모델명 매칭.
3) (선택) model_to_class.json 을 찾으면 '차종' 컬럼을 추가. 없으면 '미분류'.
4) 출력 CSV에는 최소 ['브랜드','모델명','차종'] 컬럼이 포함되며, 원본 컬럼도 유지합니다.

환경 변수(옵션)
- ALIAS_BRAND_S3  = "bucket/key/to/brand_to_fuzzy.json"   (없으면 로컬 파일 사용: ./brand_to_fuzzy.json)
- ALIAS_MODEL_S3  = "bucket/key/to/brand_norm_to_fuzzy.json" (없으면 로컬 파일 사용: ./brand_norm_to_fuzzy.json)
- MODEL_CLASS_S3  = "bucket/key/to/model_to_class.json"     (없으면 로컬 파일 사용: ./model_to_class.json, 둘 다 없으면 미분류)
- OUTPUT_BUCKET   = 결과를 저장할 S3 버킷(미지정 시 입력 버킷 사용)
"""

from __future__ import annotations
import os
import io
import json
import csv
import re
from typing import Dict, List, Tuple, Optional

import boto3

s3 = boto3.client("s3")

# -------------------- 설정 --------------------
# [수정] 다양한 CSV 형식 지원을 위해 후보 컬럼명 추가
NAME_COL_CANDIDATES = [
    "차량정보",  # autohub 형식
    "name",
    "car_name",
    "model_name",
    "title",
    "차량명",  # automart 형식
    "차명",
    "모델명",  # onbid 형식
    "full_name",
    "item_info_raw",  # onbid의 상세 정보 컬럼 (Fallback용)
]
BRAND_COL_CANDIDATES = [
    "brand",
    "브랜드",  # autohub, onbid 형식
    "제조사",  # automart 형식
    "company",
    "회사",
    "회사명",
    "메이커",
]
MODEL_COL_CANDIDATES = [
    "model",
    "모델",
    "모델명",  # onbid 형식
    "차종명",
    "등급",
    "grade",
]


# 정규화 도우미
_ws_re = re.compile(r"\s+")
_non_alnum_keep_kor = re.compile(r"[^0-9A-Za-z가-힣]+")


def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = _ws_re.sub(" ", str(s)).strip()
    s = _non_alnum_keep_kor.sub("", s)
    return s.upper()


# -------------------- JSON 로딩 --------------------


def _split_bucket_key(path: str) -> Tuple[str, str]:
    # "bucket/key1/key2.json" → (bucket, "key1/key2.json")
    p = path.strip().split("/", 1)
    if len(p) == 1:
        raise ValueError(f"잘못된 S3 경로 형식: {path}")
    return p[0], p[1]


def load_json_local_or_s3(local_path: str, env_var: str) -> Optional[dict]:
    """환경변수(env_var)에 S3경로가 있으면 S3에서 로드, 없으면 로컬(local_path) 시도.
    로컬/원격 모두 실패 시 None 반환."""
    s3_ref = os.environ.get(env_var)
    if s3_ref:
        try:
            b, k = _split_bucket_key(s3_ref)
            obj = s3.get_object(Bucket=b, Key=k)
            body = obj["Body"].read()
            return json.loads(body)
        except Exception as e:
            print(f"[WARN] {env_var} S3 로드 실패: {e}")
    # 로컬 시도
    if os.path.exists(local_path):
        try:
            with io.open(local_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] 로컬 {local_path} 로드 실패: {e}")
    return None


# -------------------- 별칭 인덱스 구축 --------------------
class AliasIndex:
    def __init__(
        self,
        brand_alias_map: Dict[str, List[str]],
        brand_model_alias_map: Dict[str, Dict[str, List[str]]],
    ):
        # 브랜드 별칭 역색인
        self.brand_alias_to_brand: Dict[str, str] = {}
        for brand, aliases in brand_alias_map.items():
            for a in [brand] + list(aliases):
                na = norm_text(a)
                if na:
                    self.brand_alias_to_brand[na] = brand

        # 모델 별칭 역색인과 브랜드→(모델→별칭들) 저장
        self.model_alias_to_pair: Dict[str, Tuple[str, str]] = {}
        self.brand_to_model_aliases: Dict[str, Dict[str, List[str]]] = {}
        for brand, model_map in brand_model_alias_map.items():
            self.brand_to_model_aliases[brand] = {}
            for model, aliases in model_map.items():
                alias_norms = []
                for a in [model] + list(aliases):
                    na = norm_text(a)
                    if not na:
                        continue
                    alias_norms.append(na)
                    self.model_alias_to_pair[na] = (brand, model)
                # 길이 내림차순 (구체적 별칭 우선)
                self.brand_to_model_aliases[brand][model] = sorted(
                    set(alias_norms), key=len, reverse=True
                )

    def match_brand(self, text: str) -> Optional[str]:
        t = norm_text(text)
        best = None
        for alias, brand in self.brand_alias_to_brand.items():
            if alias in t:
                if (best is None) or (len(alias) > len(best[0])):
                    best = (alias, brand)
        return best[1] if best else None

    def match_model(
        self, text: str, brand_hint: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        t = norm_text(text)
        # 1) 브랜드 힌트가 있으면 해당 브랜드 내에서 가장 긴 매칭
        if brand_hint and brand_hint in self.brand_to_model_aliases:
            best = None  # (len_alias, model)
            for model, aliases in self.brand_to_model_aliases[brand_hint].items():
                for alias in aliases:  # 이미 길이 내림차순
                    if alias in t:
                        cand = (len(alias), model)
                        if (best is None) or (cand[0] > best[0]):
                            best = cand
                        break  # 이 모델에 대해선 최장 별칭 매칭을 발견했으므로 다음 모델로
            if best:
                return brand_hint, best[1]
        # 2) 브랜드 힌트가 없거나 실패 시 전체 모델 별칭 역색인에서 최장 매칭
        best2 = None  # (len_alias, (brand, model))
        for alias, pair in self.model_alias_to_pair.items():
            if alias in t:
                cand = (len(alias), pair)
                if (best2 is None) or (cand[0] > best2[0]):
                    best2 = cand
        return best2[1] if best2 else (None, None)


# -------------------- 차종 매핑 --------------------


def load_model_to_class() -> Dict[str, str]:
    data = load_json_local_or_s3("model_to_class.json", "MODEL_CLASS_S3")
    if not data:
        print("[INFO] model_to_class.json 미발견 → 차종은 '미분류'로 설정")
        return {}
    # JSON은 {"모델명": "차종"}
    return {str(k): str(v) for k, v in data.items()}


# -------------------- 핵심 처리 --------------------


def get_first_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def normalize_brand_value(val: str, alias: AliasIndex) -> Optional[str]:
    if not val or str(val).strip() == "":
        return None
    # 이미 표준 브랜드(정확 표기)일 수도 있으니 우선 그대로 매핑
    normalized = alias.match_brand(str(val))
    if normalized:
        return normalized
    # 별칭으로도 못 찾았으면 None
    return None


def parse_brand_model_from_row(
    row: Dict[str, str],
    alias: AliasIndex,
    name_col: Optional[str],
    brand_col: Optional[str],
    model_col: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    # 0) 후보 텍스트 만들기 (우선순위: 모델명 컬럼 → 이름 컬럼)
    text_candidates = []
    if model_col and row.get(model_col):
        text_candidates.append(str(row.get(model_col, "")))
    if name_col and row.get(name_col):
        text_candidates.append(str(row.get(name_col, "")))
    base_text = " ".join(
        [t for t in text_candidates if t and t.lower() != "nan"]
    ).strip()

    # 1) 브랜드 우선 결정: 기존 컬럼 값이 있으면 정규화
    brand_hint = None
    if brand_col and row.get(brand_col):
        brand_hint = normalize_brand_value(row.get(brand_col, None), alias)

    # 2) 모델 탐색
    brand, model = None, None
    if base_text:
        # 브랜드 힌트가 있으면 해당 브랜드 내에서 모델 탐색
        if brand_hint:
            brand, model = alias.match_model(base_text, brand_hint)
        # 브랜드 힌트가 없으면, 텍스트에서 브랜드부터 찾고 모델 탐색
        else:
            brand_from_text = alias.match_brand(base_text)
            brand, model = alias.match_model(base_text, brand_from_text)

    # 3) 최종 보정: 브랜드는 있는데 모델을 못 찾은 경우, base_text로 다시 시도
    if brand_hint and not model and base_text:
        _, model_only = alias.match_model(base_text)
        if model_only:
            model = model_only

    # 4) 최종 보정 2: 브랜드 힌트가 있었고, 텍스트에서 찾은 브랜드가 없다면 힌트를 최종 브랜드로 사용
    if brand_hint and not brand:
        brand = brand_hint

    return brand, model


def transform_data(
    data: List[Dict[str, str]],
    header: List[str],
    alias: AliasIndex,
    model_to_class: Dict[str, str],
) -> Tuple[List[Dict[str, str]], List[str]]:
    name_col = get_first_col(header, NAME_COL_CANDIDATES)
    brand_col = get_first_col(header, BRAND_COL_CANDIDATES)
    model_col = get_first_col(header, MODEL_COL_CANDIDATES)

    for row in data:
        b, m = parse_brand_model_from_row(row, alias, name_col, brand_col, model_col)

        row["브랜드"] = b
        row["모델명"] = m
        if not m:
            row["차종"] = "미분류"
        else:
            row["차종"] = model_to_class.get(m, "미분류")

    new_header = header[:]
    for col in ["브랜드", "모델명", "차종"]:
        if col not in new_header:
            new_header.append(col)

    return data, new_header


# -------------------- S3 I/O & 파일명 처리 --------------------


def parse_input_key(key: str) -> Tuple[str, str]:
    """입력 키에서 (경매장, 날짜) 추출. 형식: [경매장]-[YYYYMMDD]-raw.csv"""
    base = key.rsplit("/", 1)[-1]
    if not base.endswith("-raw.csv"):
        raise ValueError(f"입력 파일명이 규칙과 다릅니다: {base}")
    name = base[:-8]  # remove '-raw.csv'
    parts = name.split("-")
    if len(parts) < 2:
        raise ValueError(f"입력 파일명에서 경매장/날짜를 찾을 수 없습니다: {base}")
    auction = "-".join(parts[:-1])
    yyyymmdd = parts[-1]
    if not re.fullmatch(r"\d{8}", yyyymmdd):
        raise ValueError(f"YYYYMMDD 형식이 아닙니다: {yyyymmdd}")
    return auction, yyyymmdd


def make_output_key(input_key: str) -> str:
    output_key = input_key.replace("raw/", "normal_name/", 1)
    if output_key.endswith("-raw.csv"):
        output_key = output_key[:-8] + "-normal_name.csv"
    return output_key


def read_csv_from_s3(bucket: str, key: str) -> Tuple[List[Dict[str, str]], List[str]]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(body))
    data = list(reader)
    header = reader.fieldnames if reader.fieldnames else []
    return data, header


def write_csv_to_s3(
    data: List[Dict[str, str]], header: List[str], bucket: str, key: str
):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(data)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8-sig"))


# -------------------- Lambda 핸들러 --------------------


def lambda_handler(event, context):
    try:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        raise RuntimeError(f"S3 이벤트 파싱 실패: {e}")

    brand_alias = load_json_local_or_s3("brand_to_fuzzy.json", "ALIAS_BRAND_S3")
    model_alias = load_json_local_or_s3("brand_norm_to_fuzzy.json", "ALIAS_MODEL_S3")
    if not brand_alias or not model_alias:
        raise RuntimeError(
            "brand_to_fuzzy.json / brand_norm_to_fuzzy.json 를 로드하지 못했습니다."
        )
    alias_index = AliasIndex(brand_alias, model_alias)

    model_to_class = load_model_to_class()

    data, header = read_csv_from_s3(bucket, key)
    data_out, header_out = transform_data(data, header, alias_index, model_to_class)

    out_bucket = os.environ.get("OUTPUT_BUCKET", bucket)
    out_key = make_output_key(key)

    write_csv_to_s3(data_out, header_out, out_bucket, out_key)

    return {
        "status": "ok",
        "input_bucket": bucket,
        "input_key": key,
        "output_bucket": out_bucket,
        "output_key": out_key,
        "rows": len(data_out),
    }


# -------------------- Local test helper --------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", required=True)
    ap.add_argument("--brand-alias", default="brand_to_fuzzy.json")
    ap.add_argument("--model-alias", default="brand_norm_to_fuzzy.json")
    ap.add_argument("--model-class", default="model_to_class.json")
    args = ap.parse_args()

    def load_local(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    ba = load_local(args.brand_alias)
    ma = load_local(args.model_alias)
    alias_idx = AliasIndex(ba, ma)

    mtc = {}
    if os.path.exists(args.model_class):
        mtc = load_local(args.model_class)

    with open(args.input_csv, "r", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        local_data = list(reader)
        local_header = reader.fieldnames if reader.fieldnames else []

    out_data, out_header = transform_data(local_data, local_header, alias_idx, mtc)

    with open(args.output_csv, "w", encoding="utf-8-sig", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=out_header)
        writer.writeheader()
        writer.writerows(out_data)

    print(f"Saved: {args.output_csv} rows={len(out_data)}")
