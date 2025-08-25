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
        # 쉼표를 제거하고 숫자로 변환
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
    if "LPG" in fuel_str:
        return "LPG"
    if "전기" in fuel_str:
        return "전기"
    if "하이브리드" in fuel_str:
        return "하이브리드"
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


def lambda_handler(event, context):
    """
    S3에 업로드된 auction_schedule CSV 파일을 처리하여
    RDS에 적재할 수 있는 형태로 변환하는 Lambda 함수입니다.
    """
    try:
        # S3 이벤트에서 버킷 이름과 파일 키 추출
        s3_record = event["Records"][0]["s3"]
        source_bucket = s3_record["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(
            s3_record["object"]["key"], encoding="utf-8"
        )

        print(f"[schedule-processor-v2] 처리 시작: s3://{source_bucket}/{source_key}")

        # S3 키 경로를 분석하여 경매장과 날짜 정보 추출
        key_parts = source_key.split("/")
        # auction_schedule/normal_name/[경매장]/[날짜]/[파일명]
        if (
            len(key_parts) < 5
            or key_parts[0] != "auction_schedule"
            or key_parts[1] != "normal_name"
        ):
            print(f"잘못된 경로 형식입니다: {source_key}")
            return {"statusCode": 400, "body": "잘못된 S3 키 경로입니다."}

        auction_house = key_parts[2]
        auction_date_folder = key_parts[3]

        # S3에서 CSV 파일 읽기
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        content = response["Body"].read().decode("utf-8-sig")
        csv_reader = csv.reader(io.StringIO(content))
        header = [h.strip() for h in next(csv_reader)]

        # CSV 헤더를 기반으로 열 인덱스 매핑
        col_indices = {name: i for i, name in enumerate(header)}

        # 필수 컬럼 확인
        required_cols = [
            "브랜드",
            "모델명",
            "연식",
            "변속기",
            "연료",
            "배기량",
            "주행거리",
            "색상",
            "경매날짜",
        ]
        for col in required_cols:
            if col not in col_indices:
                raise ValueError(f"필수 컬럼이 CSV 파일에 없습니다: {col}")

        processed_rows = []
        # 경매 '일정' 파일이므로 '경매상태'에 대한 필터링 없이 모든 행을 처리합니다.
        for row in csv_reader:
            # schedule_table.sql 스키마에 맞게 데이터 추출 및 변환
            new_row = [
                row[col_indices["브랜드"]],
                row[col_indices["모델명"]],
                row[col_indices.get("차종", "")],  # 'trim'에 해당, 없으면 빈 문자열
                to_int(row[col_indices["연식"]]),
                normalize_transmission(row[col_indices["변속기"]]),
                normalize_fuel(row[col_indices["연료"]]),
                to_int(row[col_indices["배기량"]]),
                to_int(row[col_indices["주행거리"]]),
                row[col_indices["색상"]],
                0,  # min_price는 현재 CSV에 없으므로 0으로 설정
                auction_house,
                row[col_indices["경매날짜"]],
            ]
            processed_rows.append(new_row)

        if not processed_rows:
            print("[schedule-processor-v2] 처리할 데이터가 없습니다.")
            return {"statusCode": 200, "body": "처리할 데이터가 없습니다."}

        # 결과 파일을 저장할 S3 경로 및 파일명 설정
        source_filename = os.path.basename(source_key)
        target_filename = source_filename.replace("-normal_name.csv", "-rds.csv")
        target_key = f"auction_schedule/processed/{auction_house}/{auction_date_folder}/{target_filename}"

        # 변환된 데이터를 CSV 포맷으로 메모리에 작성
        with io.StringIO() as csv_buffer:
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerows(processed_rows)

            # S3에 결과 파일 업로드
            s3_client.put_object(
                Bucket=source_bucket,
                Key=target_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )

        print(f"[schedule-processor-v2] 처리 완료: s3://{source_bucket}/{target_key}")
        return {
            "statusCode": 200,
            "body": f"파일 처리 성공: {source_key} -> {target_key}",
        }

    except Exception as e:
        print(f"[schedule-processor-v2] 오류 발생: {e}")
        raise e
