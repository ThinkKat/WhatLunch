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
    if "휘발유" in fuel_str:
        return "가솔린"
    if "경유" in fuel_str:
        return "디젤"
    return fuel_str


def normalize_transmission(trans_str):
    """변속기 표기를 표준화합니다."""
    if not isinstance(trans_str, str):
        return ""
    if "오토" in trans_str:
        return "자동"
    return trans_str


def lambda_handler(event, context):
    """autoinside CSV 파일을 처리하는 Lambda 함수입니다."""
    try:
        s3_record = event["Records"][0]["s3"]
        source_bucket = s3_record["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(
            s3_record["object"]["key"], encoding="utf-8"
        )

        print(f"[automart] 처리 시작: s3://{source_bucket}/{source_key}")

        key_parts = source_key.split("/")
        auction_house = key_parts[1]
        auction_date_folder = key_parts[2]

        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        content = response["Body"].read().decode("utf-8-sig")
        # csv parsing
        csv_reader = csv.reader(io.StringIO(content))
        header = [h.strip() for h in next(csv_reader)]
        col_indices = {name: i for i, name in enumerate(header)}

        auction_house = path.split("_", maxsplit=1)[0]
        processed_rows = []
        for row in csv_reader:
            new_row = [
                row[col_indices["브랜드"]],
                row[col_indices["모델명"]],
                row[col_indices.get("차종")] if "차종" in col_indices else "",
                to_int(row[col_indices["모델번호/기어"]].split("/")[0]),
                normalize_transmission(row[col_indices["변속기"]]),
                normalize_fuel(row[col_indices["연료"]]),
                to_int(row[col_indices["색상/배기량"]].split("/")[1]),
                to_int(row[col_indices["주행거리"]]),
                row[col_indices["색상"]],
                to_int(row[col_indices["winning_price"]]),
                auction_house,
                row[col_indices["입찰종료일자"]],
            ]
            processed_rows.append(new_row)

        if not processed_rows:
            print("[automart] 처리할 낙찰 데이터가 없습니다.")
            return {"statusCode": 200, "body": "처리할 낙찰 데이터가 없습니다."}

        source_filename = os.path.basename(source_key)
        target_filename = source_filename.replace("-normal_name.csv", "-rds.csv")
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

        print(f"[automart] 처리 완료: s3://{source_bucket}/{target_key}")
        return {
            "statusCode": 200,
            "body": f"파일 처리 성공: {source_key} -> {target_key}",
        }

    except Exception as e:
        print(f"[automart] 오류 발생: {e}")
        raise e
