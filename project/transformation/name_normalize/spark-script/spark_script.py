# spark_normalize.py
# 실행 예:
# spark-submit \
#  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
#  --conf spark.sql.execution.arrow.pyspark.enabled=true \
#  spark_normalize.py \
#  --input s3a://YOUR_BUCKET/raw/ \
#  --output s3a://YOUR_BUCKET/normal_name/ \
#  --brand-alias s3a://YOUR_BUCKET/ref/brand_to_fuzzy.json \
#  --model-alias s3a://YOUR_BUCKET/ref/brand_norm_to_fuzzy.json \
#  --model-class s3a://YOUR_BUCKET/ref/model_to_class.json

import json, re, argparse
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import input_file_name, regexp_extract

# ---------- 1) 별칭 인덱스 (Lambda 로직 이식) ----------
def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[\s\W_]+", "", s, flags=re.UNICODE)  # 공백/특수문자 제거
    return s

class AliasIndex:
    def __init__(self, brand_alias_map: Dict[str, List[str]],
                 brand_model_alias_map: Dict[str, Dict[str, List[str]]]):
        self.brand_alias_to_brand = {}
        for brand, aliases in brand_alias_map.items():
            for a in [brand] + list(aliases):
                na = norm_text(a)
                if na:
                    self.brand_alias_to_brand[na] = brand

        self.model_alias_to_pair = {}
        for brand, model_map in brand_model_alias_map.items():
            for model, aliases in model_map.items():
                for a in [model] + list(aliases):
                    na = norm_text(a)
                    if na:
                        self.model_alias_to_pair[na] = (brand, model)

    def find_brand(self, text: str) -> Optional[str]:
        t = norm_text(text)
        best = None
        for alias, brand in self.brand_alias_to_brand.items():
            if alias in t:
                cand = (len(alias), brand)
                if (best is None) or (cand[0] > best[0]):
                    best = cand
        return best[1] if best else None

    def find_brand_model(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        t = norm_text(text)
        best = None
        for alias, pair in self.model_alias_to_pair.items():
            if alias in t:
                cand = (len(alias), pair)
                if (best is None) or (cand[0] > best[0]):
                    best = cand
        return best[1] if best else (None, None)

# ---------- 2) 인자 & Spark ----------
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--brand-alias", required=True)
ap.add_argument("--model-alias", required=True)
ap.add_argument("--model-class", required=True)
args = ap.parse_args()

spark = (SparkSession.builder
         .appName("car-normalize")
         .getOrCreate())

sc = spark.sparkContext

# ---------- 3) 참조 JSON 로드 & 브로드캐스트 ----------
def read_json_text(path: str) -> dict:
    # s3a:// 경로도 Spark로 읽을 수 있음
    txt = spark.read.text(path).select(F.collect_list("value")).first()[0]
    # 여러 줄 JSON 파일일 경우 줄들을 합쳐 파싱
    return json.loads("\n".join(txt))

brand_alias_map = read_json_text(args.brand_alias)
brand_model_alias_map = read_json_text(args.model_alias)
model_to_class = read_json_text(args.model_class)

alias_idx = AliasIndex(brand_alias_map, brand_model_alias_map)
b_alias = sc.broadcast(alias_idx)
b_cls   = sc.broadcast(model_to_class)

# ---------- 4) 입력 스키마 & 로드 ----------
# 컬럼 후보 (Lambda와 동일 후보세트)
NAME_CANDS  = ["name","car_name","model_name","title","차량명","차명","모델명","full_name"]
BRAND_CANDS = ["brand","브랜드","company","회사","회사명","메이커"]
MODEL_CANDS = ["model","모델","모델명","차종명","등급","grade"]

schema = T.StructType([])  # 추정 가능하면 명시 스키마 권장
df = (spark.read
      .option("header", True)
      .option("multiLine", True)
      .option("escape", "\"")
      .csv(args.input))

# 파일명에서 yyyymmdd 추출 (경매장-YYYYMMDD-raw.csv)
df = df.withColumn("_src", input_file_name())
df = df.withColumn("yyyymmdd", regexp_extract(F.col("_src"), r"-([0-9]{8})-raw\.csv", 1))

# 실제 사용할 컬럼 선택 (존재하는 첫 컬럼)
def first_existing(cols):
    return F.coalesce(*[F.col(c) for c in cols if c in df.columns])

name_col  = first_existing(NAME_CANDS)
brand_col = first_existing(BRAND_CANDS)
model_col = first_existing(MODEL_CANDS)

# ---------- 5) Pandas UDF로 정규화 ----------
@F.pandas_udf("struct<브랜드:string,모델명:string,차종:string>")
def normalize_udf(name_s, brand_s, model_s):
    out_brand, out_model, out_class = [], [], []
    idx = b_alias.value
    cls = b_cls.value
    for n, b, m in zip(name_s, brand_s, model_s):
        text = (n or "") + " " + (b or "") + " " + (m or "")
        bb, mm = idx.find_brand_model(text)
        if not bb:
            bb = idx.find_brand(text)
        if not mm:
            # 브랜드만 맞고 모델은 미확정이면 모델 None
            pass
        if mm:
            cc = cls.get(mm, "미분류")
        else:
            cc = "미분류"
        out_brand.append(bb)
        out_model.append(mm)
        out_class.append(cc)
    import pandas as pd
    return pd.DataFrame({"브랜드": out_brand, "모델명": out_model, "차종": out_class})

res = df.withColumn("norm", normalize_udf(name_col, brand_col, model_col)) \
        .select("*", "norm.*") \
        .drop("norm")

# ---------- 6) 저장 ----------
# (A) CSV로 동일 경로 규칙 유지하려면: 파일 개수 조절 후 경로를 매핑 저장 필요
#    → 대량 처리에는 (B) Parquet/파티션 저장을 추천
(res
 .repartition(200, "yyyymmdd")        # 데이터량에 맞게 조정
 .write
 .mode("overwrite")
 .partitionBy("yyyymmdd")
 .parquet(args.output))

spark.stop()
