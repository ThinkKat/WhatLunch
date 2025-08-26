import os
import io
import csv
import boto3
import psycopg2
from psycopg2.extras import execute_batch

# S3 클라이언트 초기화
s3 = boto3.client("s3")

# 환경 변수에서 설정값 가져오기
TABLE = os.environ.get("TABLE_NAME", "auction_schedules")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "800"))

# 처리할 CSV 파일의 헤더 정보 (auction_schedules 테이블과 일치)
HEADER = [
    "brand",
    "model",
    "trim",
    "year",
    "transmission",
    "fuel",
    "displacement_cc",
    "mileage_km",
    "color",
    "min_price",
    "auction_house",
    "auction_date",
]

# UPSERT (INSERT ... ON CONFLICT DO UPDATE) SQL 쿼리
# uq_auction_schedules_nk 제약 조건을 기준으로 중복을 확인하고, 중복 시 일부 컬럼을 업데이트합니다.
UPSERT_SQL = f"""
INSERT INTO {TABLE}
(brand, model, trim, year, transmission, fuel, displacement_cc, mileage_km, color, min_price, auction_house, auction_date)
VALUES (%(brand)s, %(model)s, %(trim)s, %(year)s, %(transmission)s, %(fuel)s, %(displacement_cc)s, %(mileage_km)s, %(color)s, %(min_price)s, %(auction_house)s, %(auction_date)s)
ON CONFLICT ON CONSTRAINT uq_auction_schedules_nk
DO UPDATE SET
  brand         = EXCLUDED.brand,
  trim          = EXCLUDED.trim,
  transmission  = EXCLUDED.transmission,
  fuel          = EXCLUDED.fuel,
  color         = EXCLUDED.color,
  min_price     = EXCLUDED.min_price;
"""


def parse_s3_event(event):
    """Lambda 이벤트에서 S3 버킷과 키 정보를 추출합니다."""
    for rec in event.get("Records", []):
        yield rec["s3"]["bucket"]["name"], rec["s3"]["object"]["key"]


def read_csv_from_s3(bucket, key):
    """S3에서 CSV 파일을 읽고, 각 행을 딕셔너리 리스트로 변환합니다."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    rows = []
    for raw in csv.reader(io.StringIO(text)):
        if not raw or all(not c for c in raw):
            continue
        # 행의 길이가 헤더보다 짧을 경우 빈 문자열로 채움
        raw = (raw + [""] * len(HEADER))[: len(HEADER)]
        rows.append(coerce_row(dict(zip(HEADER, raw))))
    return rows


def to_int_or_none(value):
    """문자열을 정수로 변환하되, 비어있거나 변환할 수 없으면 None을 반환합니다."""
    if value is None or str(value).strip() == "":
        return None
    try:
        # 소수점이 있을 수 있는 경우를 대비해 float으로 먼저 변환 후 int로 변환
        return int(float(value))
    except (ValueError, TypeError):
        return None


def coerce_row(d):
    """CSV에서 읽은 문자열 데이터를 DB에 맞는 타입으로 안전하게 변환합니다."""
    return {
        "brand": d.get("brand"),
        "model": d.get("model"),
        "trim": d.get("trim"),
        "year": to_int_or_none(d.get("year")),
        "transmission": d.get("transmission"),
        "fuel": d.get("fuel"),
        "displacement_cc": to_int_or_none(d.get("displacement_cc")),
        "mileage_km": to_int_or_none(d.get("mileage_km")),
        "color": d.get("color"),
        "min_price": to_int_or_none(d.get("min_price")) or 0,  # None일 경우 0으로 설정
        "auction_house": d.get("auction_house"),
        "auction_date": d.get("auction_date") if d.get("auction_date") else None,
    }


def connect_db():
    """환경 변수를 사용하여 데이터베이스에 연결합니다."""
    host = os.environ["DB_HOST"]
    port = int(os.environ.get("DB_PORT", "5432"))
    db = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    pwd = os.environ["DB_PASSWORD"]
    # SSL 연결을 권장합니다.
    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=pwd, sslmode="require"
    )


def lambda_handler(event, context):
    """메인 Lambda 핸들러 함수"""
    total = 0
    print("[schedule-db-loader-v2] 처리 시작")
    with connect_db() as conn:
        with conn.cursor() as cur:
            for bucket, key in parse_s3_event(event):
                print(f"파일 처리 중: s3://{bucket}/{key}")
                rows = read_csv_from_s3(bucket, key)

                # UNIQUE 제약 조건의 모든 키 컬럼이 유효한지 확인
                valid = [
                    r
                    for r in rows
                    if all(
                        [
                            r["model"],
                            r["year"] is not None,
                            r["displacement_cc"] is not None,
                            r["mileage_km"] is not None,
                            r["auction_house"],
                            r["auction_date"],
                        ]
                    )
                ]

                if not valid:
                    print("처리할 유효한 데이터가 없습니다.")
                    continue

                # execute_batch를 사용하여 DB에 데이터 일괄 처리
                execute_batch(cur, UPSERT_SQL, valid, page_size=BATCH_SIZE)
                total += len(valid)
                print(f"{len(valid)}개 행 처리 완료.")

        conn.commit()

    print(f"[schedule-db-loader-v2] 총 {total}개 행 처리 완료.")
    return {"status": "ok", "upserted": total}
