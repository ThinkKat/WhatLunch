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


async def extract_data_from_page(page, yesterday_str):
    """
    현재 페이지에서 어제 날짜의 자동차 데이터만 추출합니다.
    다른 날짜가 나오거나 페이지 이동 오류 발생 시 신호를 보냅니다.
    """
    yesterdays_data = []
    should_stop_globally = False
    error_occurred = False

    try:
        rows = await page.locator(
            'tbody.text-center.text_vert_midd tr[role="row"]'
        ).all()

        for row in rows:
            cols = await row.locator("td").all()
            if len(cols) != 7:
                continue

            auction_date = await cols[0].inner_text()

            if auction_date != yesterday_str:
                should_stop_globally = True
                break

            car_details_html = await cols[2].inner_html()
            model_match = re.search(r"<strong>(.*?)</strong>", car_details_html)
            full_model_name = model_match.group(1).strip() if model_match else ""

            model_parts = full_model_name.split(" ", 1)
            brand = model_parts[0]
            model_info = model_parts[1] if len(model_parts) > 1 else ""

            br_match = re.search(r"<br>(.*)", car_details_html, re.DOTALL)
            other_details_text = br_match.group(1).strip() if br_match else ""
            other_details_text = re.sub(r"<.*?>", "", other_details_text)
            other_details_text = re.sub(r"\s{2,}", " ", other_details_text)
            other_details_parts = [
                part.strip() for part in other_details_text.split("|")
            ]

            while len(other_details_parts) < 5:
                other_details_parts.append("")

            price_text = await cols[6].locator("strong").inner_text()

            car_info = {
                "경매일": auction_date,
                "경매장": await cols[1].inner_text(),
                "브랜드": brand,
                "차량정보": model_info,
                "연식": other_details_parts[0],
                "변속기": other_details_parts[1],
                "연료": other_details_parts[2],
                "배기량": other_details_parts[3],
                "색상": other_details_parts[4],
                "용도": await cols[3].inner_text(),
                "주행거리": await cols[4].inner_text(),
                "평가": await cols[5].inner_text(),
                "낙찰가(만원)": price_text.replace(",", ""),
            }
            yesterdays_data.append(car_info)

    except PlaywrightError as e:
        if "Execution context was destroyed" in str(e):
            error_occurred = True
        else:
            raise e

    return yesterdays_data, should_stop_globally, error_occurred


async def main():
    """
    Playwright를 사용하여 웹 스크래핑을 수행하고 결과를 S3에 업로드하는 메인 함수
    """
    all_car_data = []
    browser = None
    local_file_name = None

    try:
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str_for_compare = yesterday.strftime("%Y-%m-%d")
        yesterday_str_for_filename = yesterday.strftime("%Y%m%d")

        print(f"🔍 어제 날짜({yesterday_str_for_compare})의 경매 데이터를 수집합니다.")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            print("🚀 페이지로 이동합니다: https://www.sellcarauction.co.kr/...")
            await page.goto(
                "https://www.sellcarauction.co.kr/newfront/successfulbid/sb/front_successfulbid_sb_list.do",
                timeout=60000,
            )

            print("✔️ '검색' 버튼을 클릭합니다.")
            await page.locator("a.button.btn_small.btn_search").click()

            await page.wait_for_selector(
                'tbody.text-center.text_vert_midd tr[role="row"]',
                state="attached",
                timeout=30000,
            )

            page_num = 1
            stop_crawling = False
            MAX_RETRIES = 3

            while not stop_crawling:
                print(f"\n--- 페이지 {page_num} 데이터 수집 시작 ---")

                success_on_page = False
                for attempt in range(MAX_RETRIES):
                    try:
                        await page.wait_for_selector(
                            'tbody.text-center.text_vert_midd tr[role="row"]',
                            state="attached",
                            timeout=10000,
                        )
                    except PlaywrightTimeoutError:
                        print(
                            f"⚠️ 페이지 {page_num} 로딩 시간 초과. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue

                    current_page_data, stop_crawling, error_occurred = (
                        await extract_data_from_page(page, yesterday_str_for_compare)
                    )

                    if error_occurred:
                        print(
                            f"⚠️ 페이지 {page_num}에서 데이터 추출 오류 발생. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue

                    success_on_page = True
                    break

                if not success_on_page:
                    print(
                        f"❌ 페이지 {page_num} 데이터 수집에 {MAX_RETRIES}번 실패하여 크롤링을 중단합니다."
                    )
                    stop_crawling = True

                if success_on_page and current_page_data:
                    all_car_data.extend(current_page_data)
                    print(
                        f"✔️ {len(current_page_data)}개 차량 정보 수집 완료. (총 {len(all_car_data)}개)"
                    )
                elif success_on_page and not stop_crawling:
                    print("⚠️ 현재 페이지에서 어제 날짜의 데이터를 찾지 못했습니다.")

                if stop_crawling:
                    break

                try:
                    active_page_element = page.locator("ul.pagination li.active a")
                    if not await active_page_element.is_visible(timeout=5000):
                        print("⭐ 페이지네이션을 찾을 수 없어 크롤링을 종료합니다.")
                        break

                    current_page_num_text = await active_page_element.inner_text()
                    current_page_num = int(current_page_num_text)
                    next_page_num = current_page_num + 1

                    next_page_button = page.locator(
                        f"//ul[contains(@class, 'pagination')]//a[text()='{next_page_num}']"
                    )

                    if await next_page_button.is_visible():
                        print(f"✔️ {next_page_num} 페이지로 이동합니다.")
                        await next_page_button.click()
                    else:
                        next_block_button = page.locator(
                            "//ul[contains(@class, 'pagination')]//a[text()='>']"
                        )
                        if await next_block_button.is_visible():
                            print("✔️ 다음 페이지 블록으로 이동합니다.")
                            await next_block_button.click()
                        else:
                            print("⭐ 마지막 페이지에 도달하여 크롤링을 종료합니다.")
                            break

                    await page.wait_for_selector(
                        'tbody.text-center.text_vert_midd tr[role="row"]',
                        state="attached",
                        timeout=30000,
                    )
                    page_num += 1
                except (PlaywrightError, ValueError) as e:
                    print(f"❌ 페이지 이동 또는 번호 확인 중 오류 발생: {e}")
                    break

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

                local_file_name = f"autohub-{yesterday_str_for_filename}-raw.csv"
                fieldnames = all_car_data[0].keys()

                with open(
                    local_file_name, "w", encoding="utf-8-sig", newline=""
                ) as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_car_data)

                print(f"✔️ 로컬 파일 '{local_file_name}'에 성공적으로 저장했습니다.")

                # --- S3 업로드 로직 ---
                s3_bucket = "whatlunch-s3"
                s3_key = f"raw/autohub/{yesterday_str_for_compare}/autohub-{yesterday_str_for_filename}-raw.csv"

                print(f" S3 버킷 '{s3_bucket}'에 업로드를 시작합니다...")
                s3_client = boto3.client("s3")
                s3_client.upload_file(local_file_name, s3_bucket, s3_key)
                print(f"✔️ S3에 성공적으로 업로드했습니다: s3://{s3_bucket}/{s3_key}")

            except Exception as e:
                print(f"❌ 파일 저장 또는 S3 업로드 중 오류 발생: {e}")

            finally:
                # --- 로컬 파일 삭제 ---
                if local_file_name and os.path.exists(local_file_name):
                    os.remove(local_file_name)
                    print(f"✔️ 로컬 임시 파일 '{local_file_name}'을 삭제했습니다.")


if __name__ == "__main__":
    asyncio.run(main())
