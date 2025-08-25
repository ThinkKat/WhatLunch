import boto3
import csv
import io
import os
import urllib.parse
import re

s3_client = boto3.client("s3")


def to_int(value):
    """문자열에서 숫자만 추출하여 정수형으로 변환합니다."""
    if value is None:
        return 0
    try:
        numeric_part = re.sub(r"[^0-9]", "", str(value))
        return int(numeric_part) if numeric_part else 0
    except (ValueError, TypeError):
        return 0


def normalize_fuel(fuel_str):
    """연료 표기를 표준화합니다."""
    if not isinstance(fuel_str, str):
        return ""
    if "휘발유" in fuel_str or "가솔린" in fuel_str:
        return "가솔린"
    if "경유" in fuel_str or "디젤" in fuel_str:
        return "디젤"
    return fuel_str


def normalize_transmission(trans_str):
    """변속기 표기를 표준화합니다."""
    if not isinstance(trans_str, str):
        return ""
    if "오토" in trans_str or "자동" in trans_str:
        return "자동"
    if "수동" in trans_str:
        return "수동"
    return trans_str


def extract_date(datetime_str):
    """'YYYY-MM-DD HH:MM' 형식에서 날짜 부분만 추출합니다."""
    if not isinstance(datetime_str, str):
        return ""
    return datetime_str.split(" ")[0]


def lambda_handler(event, context):
    """onbid CSV 파일을 처리하는 Lambda 함수입니다."""
    try:
        s3_record = event["Records"][0]["s3"]
        source_bucket = s3_record["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(
            s3_record["object"]["key"], encoding="utf-8"
        )

        print(f"[onbid] 처리 시작: s3://{source_bucket}/{source_key}")

        key_parts = source_key.split("/")
        auction_house = key_parts[1]
        auction_date_folder = key_parts[2]

        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        content = response["Body"].read().decode("utf-8-sig")
        csv_reader = csv.reader(io.StringIO(content))
        header = [h.strip() for h in next(csv_reader)]

        col_indices = {name: i for i, name in enumerate(header)}

        # 컬럼이 없을 경우를 대비하여 안전하게 인덱스를 가져옵니다.
        bid_result_idx = col_indices.get("bid_result", -1)
        # '브랜드' 컬럼을 우선적으로 찾고, 없으면 '제조사'를 찾도록 수정
        brand_idx = col_indices.get("브랜드", col_indices.get("제조사", -1))
        model_idx = col_indices.get("모델명", -1)
        trim_idx = col_indices.get("차종", -1)
        year_idx = col_indices.get("연식", -1)
        transmission_idx = col_indices.get("변속기", -1)
        fuel_idx = col_indices.get("연료", -1)
        displacement_idx = col_indices.get("배기량", -1)
        mileage_idx = col_indices.get("주행거리", -1)
        bid_price_idx = col_indices.get("bid_price", -1)
        open_datetime_idx = col_indices.get("open_datetime", -1)

        processed_rows = []
        for row in csv_reader:
            # 'bid_result' 컬럼이 존재하고 값이 '낙찰'인 경우에만 처리
            if bid_result_idx != -1 and row[bid_result_idx] == "낙찰":
                new_row = [
                    row[brand_idx] if brand_idx != -1 else "",
                    row[model_idx] if model_idx != -1 else "",
                    row[trim_idx] if trim_idx != -1 else "",
                    to_int(row[year_idx]) if year_idx != -1 else 0,
                    (
                        normalize_transmission(row[transmission_idx])
                        if transmission_idx != -1
                        else ""
                    ),
                    normalize_fuel(row[fuel_idx]) if fuel_idx != -1 else "",
                    to_int(row[displacement_idx]) if displacement_idx != -1 else 0,
                    to_int(row[mileage_idx]) if mileage_idx != -1 else 0,
                    "기타",  # 색상 컬럼은 '기타'로 고정
                    to_int(row[bid_price_idx]) if bid_price_idx != -1 else 0,
                    auction_house,
                    (
                        extract_date(row[open_datetime_idx])
                        if open_datetime_idx != -1
                        else ""
                    ),
                ]
                processed_rows.append(new_row)

        if not processed_rows:
            print("[onbid] 처리할 낙찰 데이터가 없습니다.")
            return {"statusCode": 200, "body": "처리할 낙찰 데이터가 없습니다."}

        source_filename = os.path.basename(source_key)
        # '-raw.csv'와 '-normal_name.csv'를 모두 처리할 수 있도록 수정
        target_filename = source_filename.replace("-raw.csv", "-rds.csv").replace(
            "-normal_name.csv", "-rds.csv"
        )
        target_key = (
            f"processed/{auction_house}/{auction_date_folder}/{target_filename}"
        )

        with io.StringIO() as csv_buffer:
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerows(processed_rows)
            s3_client.put_object(
                Bucket=source_bucket,
                Key=target_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )

        print(f"[onbid] 처리 완료: s3://{source_bucket}/{target_key}")
        return {
            "statusCode": 200,
            "body": f"파일 처리 성공: {source_key} -> {target_key}",
        }

    except Exception as e:
        print(f"[onbid] 오류 발생: {e}")
        raise e
