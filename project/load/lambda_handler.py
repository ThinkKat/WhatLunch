import os, io, csv, boto3, psycopg2
from psycopg2.extras import execute_batch

s3 = boto3.client("s3")

TABLE = os.environ.get("TABLE_NAME", "auction_results")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "800"))

HEADER = [
    "brand",
    "model",
    "_class_or_trim",
    "year",
    "transmission",
    "fuel",
    "displacement_cc",
    "mileage_km",
    "color",
    "winning_price",
    "auction_house",
    "auction_date",
]

UPSERT_SQL = f"""
INSERT INTO {TABLE}
(brand, model, trim, year, transmission, fuel, displacement_cc, mileage_km, color, winning_price, auction_house, auction_date)
VALUES (%(brand)s, %(model)s, %(trim)s, %(year)s, %(transmission)s, %(fuel)s, %(displacement_cc)s, %(mileage_km)s, %(color)s, %(winning_price)s, %(auction_house)s, %(auction_date)s)
ON CONFLICT ON CONSTRAINT uq_auction_results_nk
DO UPDATE SET
  brand         = EXCLUDED.brand,
  trim          = EXCLUDED.trim,
  transmission  = EXCLUDED.transmission,
  fuel          = EXCLUDED.fuel,
  color         = EXCLUDED.color,
  winning_price = EXCLUDED.winning_price,
  auction_date  = EXCLUDED.auction_date;
"""


def parse_s3_event(event):
    for rec in event.get("Records", []):
        yield rec["s3"]["bucket"]["name"], rec["s3"]["object"]["key"]


def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    rows = []
    for raw in csv.reader(io.StringIO(text)):
        if not raw or all(not c for c in raw):
            continue
        raw = (raw + [""] * len(HEADER))[: len(HEADER)]
        rows.append(coerce_row(dict(zip(HEADER, raw))))
    return rows


def coerce_row(d):
    return {
        "brand": d["brand"],
        "model": d["model"],
        "trim": d.get("_class_or_trim"),  # 세그먼트 임시 매핑
        "year": int(d["year"]) if d.get("year") else None,
        "transmission": d.get("transmission"),
        "fuel": d.get("fuel"),
        "displacement_cc": (
            int(float(d["displacement_cc"])) if d.get("displacement_cc") else None
        ),
        "mileage_km": int(float(d["mileage_km"])) if d.get("mileage_km") else None,
        "color": d.get("color"),
        "winning_price": (
            int(float(d["winning_price"])) if d.get("winning_price") else None
        ),
        "auction_house": d.get("auction_house"),
        "auction_date": d.get("auction_date"),  # YYYY-MM-DD
    }


def connect_pw():
    host = os.environ["DB_HOST"]
    port = int(os.environ.get("DB_PORT", "5432"))
    db = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    pwd = os.environ["DB_PASSWORD"]  # 비밀번호 방식
    # SSL 권장
    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=pwd, sslmode="require"
    )


def lambda_handler(event, context):
    total = 0
    with connect_pw() as conn:
        with conn.cursor() as cur:
            for bucket, key in parse_s3_event(event):
                rows = read_csv_from_s3(bucket, key)
                # 자연키 5개 구성요소는 NULL 금지
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
                        ]
                    )
                ]
                if not valid:
                    continue
                execute_batch(cur, UPSERT_SQL, valid, page_size=BATCH_SIZE)
                total += len(valid)
        conn.commit()
    return {"status": "ok", "upserted": total}
