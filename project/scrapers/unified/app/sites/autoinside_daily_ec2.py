import asyncio
import re
import csv
import os
import boto3
from datetime import datetime, timedelta
from playwright.async_api import (
    async_playwright,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
)

# --- 설정 ---
BASE_LIST_URL = "https://auction.autoinside.co.kr/auction/auction_car_end_list.do"
DETAIL_PAGE_URL_TEMPLATE = (
    "https://auction.autoinside.co.kr/auction/auction_car_view.do?i_sEntryCd={entry_cd}"
)
MAX_RETRIES = 3  # 개별 작업에 대한 최대 재시도 횟수

# t3.micro 인스턴스의 리소스 제한을 고려하여 동시 요청 수를 낮게 유지합니다.
# 안정적으로 동작한다면 4 또는 5로 조심스럽게 상향 테스트해볼 수 있습니다.
CONCURRENT_REQUESTS = 3


def clean_number(text):
    """텍스트에서 숫자만 추출하여 정수형으로 반환합니다."""
    return int(re.sub(r"[^0-9]", "", text)) if text else 0


async def block_unnecessary_resources(route):
    """t3.micro의 메모리 절약을 위해 불필요한 리소스(이미지, CSS 등) 요청을 차단합니다."""
    if route.request.resource_type in {"image", "stylesheet", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()


async def get_car_detail(context, entry_cd):
    """
    차량 상세 정보 페이지에서 상세 데이터를 추출합니다.
    각 상세 페이지는 독립된 Page 객체에서 처리하여 안정성을 높입니다.
    """
    page = await context.new_page()
    try:
        # domcontentloaded는 HTML 파싱이 완료되면 발생하여, 리소스 로딩을 기다리지 않아 더 빠릅니다.
        await page.goto(
            DETAIL_PAGE_URL_TEMPLATE.format(entry_cd=entry_cd),
            wait_until="domcontentloaded",
            timeout=45000,  # 상세 페이지 로딩 타임아웃을 넉넉하게 설정
        )
        car_data = {"entry_cd": entry_cd}

        # --- 데이터 추출 (개별 try-except로 안정성 강화) ---
        try:
            car_name_part1 = await page.locator(
                ".performance_info .car_nm .txt01"
            ).inner_text(timeout=5000)
            car_name_part2 = await page.locator(
                ".performance_info .car_nm .txt02"
            ).inner_text(timeout=5000)
            full_car_name = f"{car_name_part1} {car_name_part2}"
            parts = full_car_name.split(" ", 1)
            car_data["브랜드"] = parts[0]
            car_data["차량정보"] = parts[1] if len(parts) > 1 else ""
        except Exception:
            car_data["브랜드"] = "N/A"
            car_data["차량정보"] = "N/A"

        try:
            car_data["차량번호"] = (
                await page.locator(
                    ".fixed_detail_bid_box .car_number"
                ).first.inner_text(timeout=5000)
            ).strip()
        except Exception:
            car_data["차량번호"] = "N/A"

        try:
            info_list = await page.locator(
                ".performance_info .info_list span"
            ).all_inner_texts(timeout=5000)
            car_data["연식"] = clean_number(info_list[1])
            car_data["주행거리"] = clean_number(info_list[2])
            car_data["보관센터"] = info_list[3].strip()
        except Exception:
            car_data.update({"연식": 0, "주행거리": 0, "보관센터": "N/A"})

        try:
            announce_text = (
                await page.locator(".detail_bid_box .announce").inner_text(timeout=5000)
            ).strip()
            match = re.search(r"(\d+)월 (\d+)일", announce_text)
            if match:
                month, day = int(match.group(1)), int(match.group(2))
                # 경매 날짜가 현재 날짜보다 미래일 경우 작년으로 처리
                year = (
                    datetime.now().year
                    if (datetime.now().month > month)
                    or (datetime.now().month == month and datetime.now().day >= day)
                    else datetime.now().year - 1
                )
                car_data["경매종료일"] = f"{year}-{month:02d}-{day:02d}"
            else:
                car_data["경매종료일"] = "N/A"
        except Exception:
            car_data["경매종료일"] = "N/A"

        try:
            raw_price = (
                await page.locator(".bidding_count").inner_text(timeout=5000)
            ).strip()
            clean_price = raw_price.replace("*", "0").replace(",", "")
            price_match = re.search(r"(\d+)만원", clean_price)
            car_data["낙찰가(만원)"] = int(price_match.group(1)) if price_match else 0
        except Exception:
            car_data["낙찰가(만원)"] = 0

        return car_data
    except Exception as e:
        print(f"  - 상세 정보 추출 실패 (ID: {entry_cd}): {e}")
        return None  # 실패 시 None 반환
    finally:
        await page.close()


async def main():
    """메인 크롤링 실행 함수 (t3.micro 최적화 아키텍처)"""
    all_car_data = []
    browser = None
    local_file_name = None
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print(f"🔍 어제 날짜({yesterday_str})의 autoinside 경매 데이터 수집을 시작합니다.")

    try:
        async with async_playwright() as p:
            # headless=False로 로컬에서 실행하면 브라우저 동작을 볼 수 있습니다.
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )

            # [최적화 1] 불필요한 리소스(이미지, CSS 등)를 차단하여 메모리 사용량과 로딩 시간 단축
            await context.route("**/*", block_unnecessary_resources)

            # --- 1단계: 수집 대상 차량 ID 전체 수집 ---
            print("\n--- 1단계: 수집 대상 차량 ID 전체 수집 시작 ---")
            all_entry_cds_to_fetch = set()  # 중복 ID 방지를 위해 set 사용
            page_num = 1
            stop_id_collection = False
            list_page = await context.new_page()

            while not stop_id_collection:
                try:
                    print(f"  - ID 수집 중... (페이지 {page_num})")
                    list_page_url = f"{BASE_LIST_URL}?i_iNowPageNo={page_num}&sort=A.D_REG_DTM%20DESC"
                    await list_page.goto(
                        list_page_url, wait_until="domcontentloaded", timeout=30000
                    )

                    car_elements = await list_page.locator(
                        ".car_list_box .list li"
                    ).all()
                    if not car_elements:
                        print("  - 더 이상 차량 목록이 없어 ID 수집을 중단합니다.")
                        break

                    # [최적화 2] 페이지의 모든 차량 날짜를 확인하여 불필요한 페이지 탐색 방지
                    page_contains_target_date = False
                    for car_el in car_elements:
                        date_text = await car_el.locator(".date").inner_text(
                            timeout=5000
                        )
                        entry_cd = await car_el.locator("a.a_detail").get_attribute(
                            "data-entrycd"
                        )

                        match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", date_text)
                        if match and entry_cd:
                            car_date_str = (
                                f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                            )
                            if car_date_str == yesterday_str:
                                all_entry_cds_to_fetch.add(entry_cd)
                                page_contains_target_date = True
                            elif car_date_str < yesterday_str:
                                # 어제 이전 날짜가 나오면, 더 이상 다음 페이지를 볼 필요가 없음
                                stop_id_collection = True

                    if not page_contains_target_date and stop_id_collection:
                        print(
                            f"  - 페이지 {page_num}에서 어제 이전 날짜의 차량만 발견되어 ID 수집을 종료합니다."
                        )
                        break

                    page_num += 1

                except (PlaywrightTimeoutError, PlaywrightError) as e:
                    print(
                        f"  ⚠️ ID 수집 중 페이지 {page_num}에서 오류 발생: {e}. 다음 페이지로 넘어갑니다."
                    )
                    page_num += 1

            await list_page.close()
            print(
                f"✔️ 총 {len(all_entry_cds_to_fetch)}개의 수집 대상 ID를 발견했습니다."
            )

            # --- 2단계: 수집된 모든 ID에 대해 상세 정보 병렬 처리 ---
            if all_entry_cds_to_fetch:
                print("\n--- 2단계: 상세 정보 병렬 수집 시작 ---")
                semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
                tasks = []

                for entry_cd in all_entry_cds_to_fetch:

                    async def task_wrapper(cd):
                        async with semaphore:
                            # 상세 정보 수집 실패에 대비한 재시도 로직 추가
                            for attempt in range(MAX_RETRIES):
                                result = await get_car_detail(context, cd)
                                if result:
                                    return result
                                print(
                                    f"  - ID {cd} 재시도... ({attempt + 1}/{MAX_RETRIES})"
                                )
                                await asyncio.sleep(2)  # 재시도 전 잠시 대기
                            return None  # 최종 실패

                    tasks.append(task_wrapper(entry_cd))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                print("\n--- 3단계: 데이터 정리 및 필터링 ---")
                for res in results:
                    if isinstance(res, Exception):
                        print(f"  - 처리 중 예외 발생: {res}")
                    elif res and res.get("경매종료일") == yesterday_str:
                        all_car_data.append(res)
                print(
                    f"✔️ 최종적으로 {len(all_car_data)}개의 유효 데이터를 수집했습니다."
                )

    except Exception as e:
        print(f"\n🚨 스크립트 실행 중 예기치 않은 오류가 발생했습니다: {e}")

    finally:
        if browser:
            await browser.close()
            print("\n✔️ 브라우저가 종료되었습니다.")

        # --- 4단계: 수집된 데이터 저장 및 S3 업로드 ---
        if not all_car_data:
            print("❌ 수집된 데이터가 없어 파일 저장을 건너뜁니다.")
        else:
            print("\n💾 수집된 데이터를 저장 및 업로드합니다...")
            try:
                yesterday_str_for_filename = yesterday.strftime("%Y%m%d")
                local_file_name = f"autoinside-{yesterday_str_for_filename}-raw.csv"

                fieldnames = [
                    "경매종료일",
                    "보관센터",
                    "브랜드",
                    "차량정보",
                    "연식",
                    "차량번호",
                    "주행거리",
                    "낙찰가(만원)",
                    "entry_cd",
                ]

                with open(
                    local_file_name, "w", encoding="utf-8-sig", newline=""
                ) as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=fieldnames, extrasaction="ignore"
                    )
                    writer.writeheader()
                    writer.writerows(all_car_data)
                print(f"✔️ 로컬 파일 '{local_file_name}'에 성공적으로 저장했습니다.")

                s3_bucket = "whatlunch-s3"  # 실제 버킷 이름으로 변경하세요
                s3_key = f"raw/autoinside/{yesterday_str}/autoinside-{yesterday_str_for_filename}-raw.csv"

                print(f"  - S3 버킷 '{s3_bucket}'에 업로드를 시작합니다...")
                s3_client = boto3.client("s3")
                s3_client.upload_file(local_file_name, s3_bucket, s3_key)
                print(f"✔️ S3에 성공적으로 업로드했습니다: s3://{s3_bucket}/{s3_key}")

            except Exception as e:
                print(f"❌ 파일 저장 또는 S3 업로드 중 오류 발생: {e}")

            finally:
                if local_file_name and os.path.exists(local_file_name):
                    os.remove(local_file_name)
                    print(f"✔️ 로컬 임시 파일 '{local_file_name}'을 삭제했습니다.")


if __name__ == "__main__":
    # EC2 환경에서는 이벤트 루프 관련 문제가 발생할 수 있으므로,
    # asyncio.run() 대신 아래와 같이 명시적으로 루프를 관리하는 것이 더 안정적일 수 있습니다.
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
