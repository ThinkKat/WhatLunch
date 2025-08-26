# lambda_auction_insights.py
import os, json, datetime, io
from decimal import Decimal
import boto3, psycopg2
from psycopg2.extras import RealDictCursor

REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
OUT_BUCKET = os.environ["OUT_BUCKET"]  # s3://whatlunch-analytics
DDB_BM = os.environ["DDB_BM"]  # ar_brand_model_stats
DDB_HB = os.environ["DDB_HB"]  # ar_house_brand_stats
DDB_MO = os.environ["DDB_MO"]  # ar_monthly_avg
MIN_TRADES = int(os.environ.get("MIN_TRADES", "10"))

DB = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "5432")),
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def to_bucket_key(prefix, obj_date):
    # 예: brand_model_stats/dt=2025-08-26/result.json
    return f"{prefix}/dt={obj_date}/result.json"


def put_json_to_s3(obj, prefix, dt_str):
    bucket = OUT_BUCKET.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(OUT_BUCKET.replace("s3://", "").split("/")[1:])
    key = "/".join([p for p in [base_prefix, to_bucket_key(prefix, dt_str)] if p])
    body = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    s3.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8"
    )
    return f"s3://{bucket}/{key}"


def fetch_all(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def run_queries(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # A) 브랜드×모델 요약
        sql_bm = f"""
        SELECT brand, model,
               AVG(winning_price)::numeric AS avg_price,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY winning_price)::numeric AS p50_price,
               percentile_cont(0.9) WITHIN GROUP (ORDER BY winning_price)::numeric AS p90_price,
               COUNT(*)::bigint AS trades,
               AVG(mileage_km)::numeric AS avg_mileage,
               MAX(auction_date) AS last_date
        FROM public.auction_results
        GROUP BY brand, model
        HAVING COUNT(*) >= %s
        ORDER BY avg_price DESC;
        """
        bm = fetch_all(cur, sql_bm, (MIN_TRADES,))

        # B) 경매장×브랜드 요약
        sql_hb = """
        SELECT auction_house, brand,
               AVG(winning_price)::numeric AS avg_price,
               COUNT(*)::bigint AS trades,
               MAX(auction_date) AS last_date
        FROM public.auction_results
        GROUP BY auction_house, brand
        ORDER BY auction_house, avg_price DESC;
        """
        hb = fetch_all(cur, sql_hb)

        # C) 월별 평균
        sql_mo = """
        SELECT to_char(date_trunc('month', auction_date), 'YYYY-MM') AS month,
               AVG(winning_price)::numeric AS avg_price,
               COUNT(*)::bigint AS trades
        FROM public.auction_results
        GROUP BY 1
        ORDER BY 1;
        """
        mo = fetch_all(cur, sql_mo)

        return bm, hb, mo


def to_decimal(x):
    # psycopg2 numeric -> Decimal은 자동이지만, None 처리용
    return Decimal(str(x)) if x is not None else None


def to_ddb_items_brand_model(bm):
    items = []
    for r in bm:
        items.append(
            {
                "pk": f"{r['brand']}#{r['model']}",
                "avg_price": to_decimal(r["avg_price"]),
                "p50_price": to_decimal(r["p50_price"]),
                "p90_price": to_decimal(r["p90_price"]),
                "trades": int(r["trades"]),
                "avg_mileage": to_decimal(r["avg_mileage"]),
                "last_date": str(r["last_date"]) if r["last_date"] else None,
            }
        )
    return items


def to_ddb_items_house_brand(hb):
    items = []
    for r in hb:
        items.append(
            {
                "auction_house": r["auction_house"],
                "brand": r["brand"],
                "avg_price": to_decimal(r["avg_price"]),
                "trades": int(r["trades"]),
                "last_date": str(r["last_date"]) if r["last_date"] else None,
            }
        )
    return items


def to_ddb_items_monthly(mo):
    items = []
    for r in mo:
        items.append(
            {
                "month": r["month"],
                "avg_price": to_decimal(r["avg_price"]),
                "trades": int(r["trades"]),
            }
        )
    return items


def ddb_batch_put(table_name, items, pkeys):
    table = dynamodb.Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
        for it in items:
            batch.put_item(Item=it)


def lambda_handler(event, context):
    dt_str = datetime.date.today().isoformat()

    # 1) DB 연결
    conn = psycopg2.connect(**DB, sslmode="require")
    try:
        bm, hb, mo = run_queries(conn)
    finally:
        conn.close()

    # 2) S3 저장
    s3_bm = put_json_to_s3(bm, "brand_model_stats", dt_str)
    s3_hb = put_json_to_s3(hb, "house_brand_stats", dt_str)
    s3_mo = put_json_to_s3(mo, "monthly_avg", dt_str)

    # 3) DynamoDB 업서트
    ddb_batch_put(DDB_BM, to_ddb_items_brand_model(bm), ["pk"])
    ddb_batch_put(DDB_HB, to_ddb_items_house_brand(hb), ["auction_house", "brand"])
    ddb_batch_put(DDB_MO, to_ddb_items_monthly(mo), ["month"])

    return {
        "status": "ok",
        "s3_outputs": {"bm": s3_bm, "hb": s3_hb, "mo": s3_mo},
        "counts": {"bm": len(bm), "hb": len(hb), "mo": len(mo)},
    }
