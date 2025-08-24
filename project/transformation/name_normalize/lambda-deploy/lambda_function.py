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
# 차량 이름 컬럼 후보 (존재하는 첫 번째 사용)
NAME_COL_CANDIDATES = [
    "name",
    "car_name",
    "model_name",
    "title",
    "차량명",
    "차명",
    "모델명",
    "full_name",
]
# 브랜드/회사명 컬럼 후보 (이미 있는 경우 우선 사용)
BRAND_COL_CANDIDATES = ["brand", "브랜드", "company", "회사", "회사명", "메이커"]
# 모델명 컬럼 후보 (이미 있는 경우 우선 사용)
MODEL_COL_CANDIDATES = ["model", "모델", "모델명", "차종명", "등급", "grade"]

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
    row: Dict[str, str],  # PANDAS-FREE CHANGE: pd.Series 대신 Dict
    alias: AliasIndex,
    name_col: Optional[str],
    brand_col: Optional[str],
    model_col: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    # 0) 후보 텍스트 만들기 (우선순위: 모델명 컬럼 → 이름 컬럼)
    text_candidates = []
    if model_col:
        text_candidates.append(row.get(model_col, ""))
    if name_col:
        text_candidates.append(row.get(name_col, ""))
    base_text = " ".join([t for t in text_candidates if t and t != "nan"]).strip()

    # 1) 브랜드 우선 결정: 기존 컬럼 값이 있으면 정규화
    brand_hint = None
    if brand_col:
        brand_hint = normalize_brand_value(row.get(brand_col, None), alias)

    # 2) 모델 탐색
    if base_text:
        if brand_hint:
            brand, model = alias.match_model(base_text, brand_hint)
        else:
            brand = alias.match_brand(base_text)
            brand, model = alias.match_model(base_text, brand)
    else:
        brand, model = (brand_hint, None)

    # 3) 브랜드 컬럼이 있었고, 정규화된 브랜드가 있고, 모델만 비었으면 모델만 재시도
    if brand_hint and (not brand) and (base_text):
        _, model2 = alias.match_model(base_text, brand_hint)
        if model2:
            brand, model = brand_hint, model2

    return brand, model


def transform_data(
    data: List[Dict[str, str]],  # PANDAS-FREE CHANGE: 데이터 구조 변경
    header: List[str],  # PANDAS-FREE CHANGE: 헤더 추가
    alias: AliasIndex,
    model_to_class: Dict[str, str],
) -> Tuple[List[Dict[str, str]], List[str]]:  # PANDAS-FREE CHANGE: 리턴 타입 변경
    name_col = get_first_col(header, NAME_COL_CANDIDATES)
    brand_col = get_first_col(header, BRAND_COL_CANDIDATES)
    model_col = get_first_col(header, MODEL_COL_CANDIDATES)

    for row in data:
        b, m = parse_brand_model_from_row(row, alias, name_col, brand_col, model_col)

        # PANDAS-FREE CHANGE: 새로운 컬럼을 row 딕셔너리에 직접 추가
        row["브랜드"] = b
        row["모델명"] = m
        if not m:
            row["차종"] = "미분류"
        else:
            row["차종"] = model_to_class.get(m, "미분류")

    # PANDAS-FREE CHANGE: 출력 파일에 새로운 컬럼이 포함되도록 헤더 업데이트
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
    auction = "-".join(parts[:-1])  # 경매장에 '-'가 포함될 수도 있음
    yyyymmdd = parts[-1]
    if not re.fullmatch(r"\d{8}", yyyymmdd):
        raise ValueError(f"YYYYMMDD 형식이 아닙니다: {yyyymmdd}")
    return auction, yyyymmdd


def make_output_key(input_key: str) -> str:
    # 1. 파일명(-raw.csv -> -normal_name.csv)과 경로(raw -> normal_name)를 한 번에 변경
    # 'raw/' 를 'normal_name/' 으로 첫 번째 한 번만 교체합니다.
    output_key = input_key.replace("raw/", "normal_name/", 1)

    # 2. 파일명의 끝부분을 교체합니다.
    if output_key.endswith("-raw.csv"):
        output_key = output_key[:-8] + "-normal_name.csv"

    return output_key


# PANDAS-FREE CHANGE: pd.read_csv 대체
def read_csv_from_s3(bucket: str, key: str) -> Tuple[List[Dict[str, str]], List[str]]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8-sig")  # BOM 처리 포함
    reader = csv.DictReader(io.StringIO(body))
    data = list(reader)
    header = reader.fieldnames if reader.fieldnames else []
    return data, header


# PANDAS-FREE CHANGE: df.to_csv 대체
def write_csv_to_s3(
    data: List[Dict[str, str]], header: List[str], bucket: str, key: str
):
    # 엑셀/윈도우 호환을 위해 BOM 포함 UTF-8 권장
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(data)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8-sig"))


# -------------------- Lambda 핸들러 --------------------


def lambda_handler(event, context):
    # 1) 이벤트에서 S3 bucket/key 추출 (ObjectCreated)
    try:
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
    except Exception as e:
        raise RuntimeError(f"S3 이벤트 파싱 실패: {e}")

    # 2) 별칭/차종 데이터 로딩
    brand_alias = load_json_local_or_s3("brand_to_fuzzy.json", "ALIAS_BRAND_S3")
    model_alias = load_json_local_or_s3("brand_norm_to_fuzzy.json", "ALIAS_MODEL_S3")
    if not brand_alias or not model_alias:
        raise RuntimeError(
            "brand_to_fuzzy.json / brand_norm_to_fuzzy.json 를 로드하지 못했습니다."
        )
    alias_index = AliasIndex(brand_alias, model_alias)

    model_to_class = load_model_to_class()  # 없으면 {}

    # 3) CSV 읽기 → 변환
    data, header = read_csv_from_s3(bucket, key)
    data_out, header_out = transform_data(data, header, alias_index, model_to_class)

    # 4) 출력 버킷/키 결정
    out_bucket = os.environ.get("OUTPUT_BUCKET", bucket)
    out_key = make_output_key(key)

    # 5) 저장
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
    # 로컬 테스트는 pandas를 사용하지 않으므로 더 이상 필요하지 않습니다.
    # 필요하다면 아래와 같이 내장 csv 모듈을 사용하여 구현할 수 있습니다.
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

    # PANDAS-FREE LOCAL TEST
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
