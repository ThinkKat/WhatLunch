import asyncio
import re
import csv
import io
import os
import boto3
import logging
from logging import handlers
from datetime import datetime, timedelta, timezone
from playwright.async_api import (
    async_playwright,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
)

# === 기본 설정 ===
SITE = "autohub"
BUCKET = os.environ.get("BUCKET", "whatlunch-s3")  # 기본 버킷 이름, 필요시 변경
LOG_PREFIX = os.environ.get("LOG_PREFIX", f"logs/{SITE}")
LOG_DIR = f"/app/logs/{SITE}"
os.makedirs(LOG_DIR, exist_ok=True)

# --- 시간대 및 날짜 설정 ---
KST = timezone(timedelta(hours=9))
yesterday_dt = datetime.now(KST) - timedelta(days=1)
yesterday_folder = yesterday_dt.strftime("%Y-%m-%d")
yesterday_file = yesterday_dt.strftime("%Y%m%d")

LOG_FILE = os.path.join(LOG_DIR, f"crawl_{yesterday_file}.log")

# --- 로깅 설정 (콘솔 + 파일) ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# 기존 핸들러 제거
for h in list(logger.handlers):
    logger.removeHandler(h)

fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# 콘솔 핸들러
sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)

# 파일 핸들러
fh = handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
fh.setFormatter(fmt)
logger.addHandler(fh)


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
            logger.error(f"Playwright 오류 발생: {e}")
            raise e

    return yesterdays_data, should_stop_globally, error_occurred


async def save_data_to_s3(data, target_date):
    """
    수집된 데이터를 S3에 CSV 파일로 업로드합니다.
    """
    if not data:
        logger.warning("S3에 업로드할 데이터가 없습니다.")
        return

    folder_date = target_date.strftime("%Y-%m-%d")
    file_date = target_date.strftime("%Y%m%d")
    s3_key = f"raw/{SITE}/{folder_date}/{SITE}-{file_date}-raw.csv"

    logger.info(
        f"수집된 {len(data)}개 데이터를 s3://{BUCKET}/{s3_key} 경로에 업로드합니다."
    )

    csv_buffer = io.StringIO()
    # 데이터의 첫 번째 항목을 기반으로 필드 이름 동적 생성
    fieldnames = list(data[0].keys()) if data else []

    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

    s3 = boto3.client("s3")
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8-sig"),
        )
        logger.info("✔️ S3 업로드 완료!")
    except Exception as e:
        logger.error(f"❌ S3 업로드 중 오류 발생: {e}")


async def main():
    """
    Playwright를 사용하여 웹 스크래핑을 수행하고 결과를 S3에 저장하는 메인 함수
    """
    all_car_data = []
    browser = None

    try:
        yesterday_str_for_compare = yesterday_dt.strftime("%Y-%m-%d")
        logger.info(
            f"🔍 어제 날짜({yesterday_str_for_compare})의 경매 데이터를 수집합니다."
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info("🚀 페이지로 이동합니다: https://www.sellcarauction.co.kr/...")
            await page.goto(
                "https://www.sellcarauction.co.kr/newfront/successfulbid/sb/front_successfulbid_sb_list.do",
                timeout=60000,
            )

            logger.info("✔️ '검색' 버튼을 클릭합니다.")
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
                logger.info(f"--- 페이지 {page_num} 데이터 수집 시작 ---")

                success_on_page = False
                for attempt in range(MAX_RETRIES):
                    try:
                        await page.wait_for_selector(
                            'tbody.text-center.text_vert_midd tr[role="row"]',
                            state="attached",
                            timeout=10000,
                        )
                    except PlaywrightTimeoutError:
                        logger.warning(
                            f"⚠️ 페이지 {page_num} 로딩 시간 초과. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue

                    current_page_data, stop_crawling, error_occurred = (
                        await extract_data_from_page(page, yesterday_str_for_compare)
                    )

                    if error_occurred:
                        logger.warning(
                            f"⚠️ 페이지 {page_num}에서 데이터 추출 오류 발생. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue

                    success_on_page = True
                    break

                if not success_on_page:
                    logger.error(
                        f"❌ 페이지 {page_num} 데이터 수집에 {MAX_RETRIES}번 실패하여 크롤링을 중단합니다."
                    )
                    stop_crawling = True

                if success_on_page and current_page_data:
                    all_car_data.extend(current_page_data)
                    logger.info(
                        f"✔️ {len(current_page_data)}개 차량 정보 수집 완료. (총 {len(all_car_data)}개)"
                    )
                elif success_on_page and not stop_crawling:
                    logger.info(
                        "✔️ 현재 페이지에서 어제 날짜의 데이터를 찾지 못했습니다."
                    )

                if stop_crawling:
                    break

                # --- 페이지네이션 처리 ---
                try:
                    active_page_element = page.locator("ul.pagination li.active a")
                    if not await active_page_element.is_visible(timeout=5000):
                        logger.info(
                            "⭐ 페이지네이션을 찾을 수 없어 크롤링을 종료합니다."
                        )
                        break

                    current_page_num_text = await active_page_element.inner_text()
                    current_page_num = int(current_page_num_text)
                    next_page_num = current_page_num + 1

                    next_page_button = page.locator(
                        f"//ul[contains(@class, 'pagination')]//a[text()='{next_page_num}']"
                    )

                    if await next_page_button.is_visible():
                        logger.info(f"✔️ {next_page_num} 페이지로 이동합니다.")
                        await next_page_button.click()
                    else:
                        next_block_button = page.locator(
                            "//ul[contains(@class, 'pagination')]//a[text()='>']"
                        )
                        if await next_block_button.is_visible():
                            logger.info("✔️ 다음 페이지 블록으로 이동합니다.")
                            await next_block_button.click()
                        else:
                            logger.info(
                                "⭐ 마지막 페이지에 도달하여 크롤링을 종료합니다."
                            )
                            break

                    await page.wait_for_selector(
                        'tbody.text-center.text_vert_midd tr[role="row"]',
                        state="attached",
                        timeout=30000,
                    )
                    page_num += 1
                except (PlaywrightError, ValueError) as e:
                    logger.error(f"❌ 페이지 이동 또는 번호 확인 중 오류 발생: {e}")
                    break

    except Exception as e:
        logger.critical(f"\n🚨 예기치 않은 오류가 발생했습니다: {e}", exc_info=True)

    finally:
        if browser:
            await browser.close()
            logger.info("\n✔️ 브라우저가 종료되었습니다.")

        # 데이터 S3에 저장
        await save_data_to_s3(all_car_data, yesterday_dt)

    # --- 로그 업로드 ---
    try:
        log_s3_key = f"{LOG_PREFIX}/{yesterday_folder}/crawl_{yesterday_file}.log"
        boto3.client("s3").upload_file(LOG_FILE, BUCKET, log_s3_key)
        logger.info(f"✔️ 로그 업로드 완료: s3://{BUCKET}/{log_s3_key}")
    except Exception as e:
        logger.error(f"❌ 로그 업로드 실패: {e}")


if __name__ == "__main__":
    asyncio.run(main())
