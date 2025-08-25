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
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 5  # 상세 페이지 동시 요청 수


def clean_number(text):
    """텍스트에서 숫자만 추출하여 정수형으로 반환합니다."""
    return int(re.sub(r"[^0-9]", "", text)) if text else 0


async def get_car_detail(context, entry_cd):
    """차량 상세 정보 페이지에서 상세 데이터를 추출합니다."""
    page = await context.new_page()
    try:
        await page.goto(
            DETAIL_PAGE_URL_TEMPLATE.format(entry_cd=entry_cd),
            wait_until="domcontentloaded",
            timeout=30000,
        )
        car_data = {"entry_cd": entry_cd}

        # --- 데이터 추출 (개별 try-except로 안정성 강화) ---
        try:
            car_name_part1 = await page.locator(
                ".performance_info .car_nm .txt01"
            ).inner_text()
            car_name_part2 = await page.locator(
                ".performance_info .car_nm .txt02"
            ).inner_text()
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
                ).first.inner_text()
            ).strip()
        except Exception:
            car_data["차량번호"] = "N/A"

        try:
            info_list = await page.locator(
                ".performance_info .info_list span"
            ).all_inner_texts()
            car_data["연식"] = clean_number(info_list[1])
            car_data["주행거리"] = clean_number(info_list[2])
            car_data["보관센터"] = info_list[3].strip()
        except Exception:
            car_data.update({"연식": 0, "주행거리": 0, "보관센터": "N/A"})

        try:
            announce_text = (
                await page.locator(".detail_bid_box .announce").inner_text()
            ).strip()
            match = re.search(r"(\d+)월 (\d+)일", announce_text)
            if match:
                month, day = int(match.group(1)), int(match.group(2))
                year = (
                    datetime.now().year
                    if datetime.now().month >= month
                    else datetime.now().year - 1
                )
                car_data["경매종료일"] = f"{year}-{month:02d}-{day:02d}"
            else:
                car_data["경매종료일"] = "N/A"
        except Exception:
            car_data["경매종료일"] = "N/A"

        try:
            raw_price = (await page.locator(".bidding_count").inner_text()).strip()
            clean_price = raw_price.replace("*", "0").replace(",", "")
            price_match = re.search(r"(\d+)만원", clean_price)
            car_data["낙찰가(만원)"] = int(price_match.group(1)) if price_match else 0
        except Exception:
            car_data["낙찰가(만원)"] = 0

        return car_data
    finally:
        await page.close()


async def fetch_ids_from_page(page, page_num):
    """지정된 페이지에서 모든 차량 ID(entry_cd)를 수집합니다."""
    list_page_url = f"{BASE_LIST_URL}?i_iNowPageNo={page_num}&sort=A.D_REG_DTM%20DESC"
    await page.goto(list_page_url, timeout=30000)
    await page.wait_for_selector(
        ".car_list_box .list li:first-child", state="attached", timeout=20000
    )
    links = await page.locator(".car_list_box .list li a.a_detail").all()
    return [
        await link.get_attribute("data-entrycd")
        for link in links
        if await link.get_attribute("data-entrycd")
    ]


async def main():
    """메인 크롤링 실행 함수"""
    all_car_data = []
    browser = None
    local_file_name = None

    try:
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str_for_compare = yesterday.strftime("%Y-%m-%d")

        print(
            f"🔍 어제 날짜({yesterday_str_for_compare})의 autoinside 경매 데이터를 수집합니다."
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            list_page = await context.new_page()

            page_num = 1
            stop_crawling = False

            while not stop_crawling:
                print(f"\n--- 페이지 {page_num} 데이터 수집 시작 ---")
                success_on_page = False
                for attempt in range(MAX_RETRIES):
                    try:
                        entry_cds_on_page = await fetch_ids_from_page(
                            list_page, page_num
                        )
                        if not entry_cds_on_page:
                            print("더 이상 차량 정보가 없어 크롤링을 종료합니다.")
                            stop_crawling = True
                            success_on_page = True
                            break

                        print(
                            f"{len(entry_cds_on_page)}개 ID 수집 완료. 상세 정보 확인을 시작합니다."
                        )

                        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
                        tasks = []
                        for entry_cd in entry_cds_on_page:

                            async def task_wrapper(cd):
                                async with semaphore:
                                    return await get_car_detail(context, cd)

                            tasks.append(task_wrapper(entry_cd))

                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        page_has_yesterday_data = False
                        for car_data in results:
                            if isinstance(car_data, Exception) or not car_data:
                                continue

                            end_date_str = car_data.get("경매종료일", "N/A")
                            if end_date_str == yesterday_str_for_compare:
                                all_car_data.append(car_data)
                                page_has_yesterday_data = True
                            elif (
                                end_date_str < yesterday_str_for_compare
                                and end_date_str != "N/A"
                            ):
                                print(
                                    f"어제 이전 날짜({end_date_str})의 차량을 발견하여 크롤링을 중단합니다."
                                )
                                stop_crawling = True

                        success_on_page = True
                        break  # 성공 시 재시도 루프 탈출

                    except (PlaywrightTimeoutError, PlaywrightError) as e:
                        print(
                            f"⚠️ 페이지 {page_num} 처리 중 오류 발생. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        if attempt < MAX_RETRIES - 1:
                            await list_page.reload()
                        else:
                            print(
                                f"❌ 페이지 {page_num} 데이터 수집에 최종 실패하여 크롤링을 중단합니다."
                            )
                            stop_crawling = True

                if stop_crawling or not success_on_page:
                    break

                page_num += 1

    except Exception as e:
        print(f"\n🚨 예기치 않은 오류가 발생했습니다: {e}")

    finally:
        if browser:
            await browser.close()
            print("\n✔️ 브라우저가 종료되었습니다.")

        if not all_car_data:
            print("❌ 수집된 데이터가 없어 파일 저장을 건너뜁니다.")
        else:
            print("\n💾 현재까지 수집된 데이터를 저장 및 업로드합니다...")
            try:
                yesterday = datetime.now() - timedelta(days=1)
                yesterday_str_for_compare = yesterday.strftime("%Y-%m-%d")
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

                s3_bucket = "whatlunch-s3"
                s3_key = f"raw/autoinside/{yesterday_str_for_compare}/autoinside-{yesterday_str_for_filename}-raw.csv"

                print(f" S3 버킷 '{s3_bucket}'에 업로드를 시작합니다...")
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
    asyncio.run(main())
