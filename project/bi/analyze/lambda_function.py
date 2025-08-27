import os
import json
import datetime
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

# --- 환경 변수 설정 ---
REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
OUT_BUCKET = os.environ["OUT_BUCKET"]  # 예: s3://whatlunch-s3/analytic-bi
DDB_BM = os.environ["DDB_BM"]  # ar_brand_model_stats
DDB_HB = os.environ["DDB_HB"]  # ar_house_brand_stats
DDB_MO = os.environ["DDB_MO"]  # ar_monthly_avg
MIN_TRADES = int(os.environ.get("MIN_TRADES", "10"))

# 데이터베이스 연결 정보
DB_CONFIG = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "5432")),
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)

# Boto3 클라이언트 및 리소스 초기화
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def put_json_to_s3(data, prefix, dt_str):
    """분석 결과를 S3 버킷에 JSON 파일로 저장합니다."""
    # S3 경로에서 버킷 이름과 기본 접두사(prefix)를 분리합니다.
    bucket = OUT_BUCKET.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(OUT_BUCKET.replace("s3://", "").split("/")[1:])

    # 최종 파일 키를 생성합니다.
    key = f"{base_prefix}/{prefix}/{dt_str}/result.json"

    # 데이터를 JSON 문자열로 변환하고 UTF-8로 인코딩합니다.
    body = (json.dumps(data, ensure_ascii=False, default=str) + "\n").encode("utf-8")

    s3.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8"
    )
    print(f"S3에 저장 완료: s3://{bucket}/{key}")
    return f"s3://{bucket}/{key}"


def run_queries(conn):
    """데이터베이스에 연결하여 세 가지 통계 쿼리를 실행합니다."""
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
        ORDER BY brand, model;
        """
        cur.execute(sql_bm, (MIN_TRADES,))
        bm_stats = cur.fetchall()

        # B) 경매장×브랜드 요약
        sql_hb = """
        SELECT auction_house, brand,
               AVG(winning_price)::numeric AS avg_price,
               COUNT(*)::bigint AS trades,
               MAX(auction_date) AS last_date
        FROM public.auction_results
        GROUP BY auction_house, brand
        ORDER BY auction_house, brand;
        """
        cur.execute(sql_hb)
        hb_stats = cur.fetchall()

        # C) 월별 평균
        sql_mo = """
        SELECT to_char(date_trunc('month', auction_date), 'YYYY-MM') AS month,
               AVG(winning_price)::numeric AS avg_price,
               COUNT(*)::bigint AS trades
        FROM public.auction_results
        GROUP BY 1
        ORDER BY 1;
        """
        cur.execute(sql_mo)
        mo_stats = cur.fetchall()

        return bm_stats, hb_stats, mo_stats


def ddb_batch_put(table_name, items, pkeys):
    """데이터 목록을 DynamoDB 테이블에 배치 쓰기 작업을 통해 저장합니다."""
    table = dynamodb.Table(table_name)
    with table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
        for item in items:
            # DynamoDB는 float 타입을 지원하지 않으므로 Decimal 타입으로 변환합니다.
            # 또한, JSON 직렬화가 불가능한 객체들을 처리합니다.
            processed_item = json.loads(
                json.dumps(item, default=str), parse_float=Decimal
            )
            batch.put_item(Item=processed_item)


def lambda_handler(event, context):
    """Lambda 함수의 메인 핸들러입니다."""
    dt_str = datetime.date.today().isoformat()
    print(f"작업 시작: {dt_str}")

    conn = None
    try:
        # 1. 데이터베이스 연결 및 쿼리 실행
        print("데이터베이스에 연결합니다...")
        conn = psycopg2.connect(**DB_CONFIG, sslmode="require")
        bm, hb, mo = run_queries(conn)
        print(
            f"쿼리 완료: brand_model={len(bm)}, house_brand={len(hb)}, monthly={len(mo)}"
        )
    except Exception as e:
        print(f"데이터베이스 작업 중 오류 발생: {e}")
        raise e
    finally:
        if conn:
            conn.close()
            print("데이터베이스 연결을 닫았습니다.")

    # 2. S3에 결과 저장
    s3_outputs = {}
    try:
        print("결과를 S3에 저장합니다...")
        s3_outputs["bm"] = put_json_to_s3(bm, "brand_model_stats", dt_str)
        s3_outputs["hb"] = put_json_to_s3(hb, "house_brand_stats", dt_str)
        s3_outputs["mo"] = put_json_to_s3(mo, "monthly_avg", dt_str)
    except Exception as e:
        print(f"S3 저장 중 오류 발생: {e}")
        raise e

    # 3. DynamoDB에 결과 저장
    try:
        print("결과를 DynamoDB에 저장합니다...")
        # DynamoDB 저장을 위해 데이터 포맷을 일부 변환합니다.
        bm_items = [{"pk": f"{r['brand']}#{r['model']}", **r} for r in bm]

        ddb_batch_put(DDB_BM, bm_items, ["pk"])
        ddb_batch_put(DDB_HB, hb, ["auction_house", "brand"])
        ddb_batch_put(DDB_MO, mo, ["month"])
        print("DynamoDB 저장 완료.")
    except Exception as e:
        print(f"DynamoDB 저장 중 오류 발생: {e}")
        raise e

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "status": "ok",
                "s3_outputs": s3_outputs,
                "counts": {"bm": len(bm), "hb": len(hb), "mo": len(mo)},
            }
        ),
    }
