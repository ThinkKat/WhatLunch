import asyncio
import re
import csv
import os
import boto3
from datetime import datetime, timedelta, timezone
from playwright.async_api import (
    async_playwright,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
)
import logging
from logging import handlers

# === 설정 ===
SITE = "autohub"
BUCKET = os.environ.get("BUCKET", "whatlunch-s3")
LOG_PREFIX = os.environ.get("LOG_PREFIX", f"logs/{SITE}")
LOG_DIR = f"/app/logs/{SITE}"
os.makedirs(LOG_DIR, exist_ok=True)

# KST(UTC+9)
KST = timezone(timedelta(hours=9))
# yesterday_dt = datetime.now(KST) - timedelta(days=1)
# today_mode: 필요 시 오늘로 테스트
# yesterday_dt = datetime.now(KST)
yesterday_dt = datetime.now(KST) - timedelta(days=1)
yesterday_folder = yesterday_dt.strftime("%Y-%m-%d")
yesterday_file = yesterday_dt.strftime("%Y%m%d")

LOG_FILE = os.path.join(LOG_DIR, f"crawl_{yesterday_file}.log")

# --- 로깅 설정: 콘솔 + 파일 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# 기존 핸들러 제거 후 재설정
for h in list(logger.handlers):
    logger.removeHandler(h)

fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)

fh = handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
fh.setFormatter(fmt)
logger.addHandler(fh)


async def extract_data_from_page(page, yesterday_str):
    yesterdays_data = []
    should_stop_globally = False
    error_occurred = False
    try:
        rows = await page.locator(
            'tbody.text-center.text_vert_midd tr[role="row"]'
        ).all()
        if not rows:
            logging.warning("현재 페이지에서 데이터 행을 찾을 수 없습니다.")
            return [], False, False
        for row in rows:
            cols = await row.locator("td").all()
            if len(cols) != 7:
                continue
            auction_date = await cols[0].inner_text()
            if auction_date != yesterday_str:
                logging.info(
                    f"어제({yesterday_str})와 다른 날짜({auction_date})의 데이터를 발견하여 수집을 중단합니다."
                )
                should_stop_globally = True
                break
            car_details_html = await cols[2].inner_html()
            model_match = re.search(r"<strong>(.*?)</strong>", car_details_html)
            full_model_name = model_match.group(1).strip() if model_match else ""
            model_parts = full_model_name.split(" ", 1)
            brand = model_parts[0]
            model_info = model_parts[1] if len(model_parts) > 1 else ""
            import re as _re

            br_match = _re.search(r"<br>(.*)", car_details_html, _re.DOTALL)
            other_details_text = br_match.group(1).strip() if br_match else ""
            other_details_text = _re.sub(r"<.*?>", "", other_details_text)
            other_details_text = _re.sub(r"\s{2,}", " ", other_details_text)
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
            logging.warning("페이지 이동 중 컨텍스트가 파괴되어 재시도합니다.")
            error_occurred = True
        else:
            raise e
    return yesterdays_data, should_stop_globally, error_occurred


async def main():
    """Autohub 크롤링 + CSV 업로드 + 로그 업로드"""
    all_car_data = []
    browser = None
    local_file_name = None
    try:
        yesterday_str_for_compare = yesterday_folder
        logging.info(
            f"🔍 어제 날짜({yesterday_str_for_compare})의 Autohub 경매 데이터를 수집합니다."
        )
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            page = await browser.new_page()
            logging.info("🚀 페이지로 이동합니다: https://www.sellcarauction.co.kr/...")
            await page.goto(
                "https://www.sellcarauction.co.kr/newfront/successfulbid/sb/front_successfulbid_sb_list.do",
                timeout=60000,
            )
            logging.info("✔️ '검색' 버튼을 클릭합니다.")
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
                logging.info(f"--- 페이지 {page_num} 데이터 수집 시작 ---")
                success_on_page = False
                for attempt in range(MAX_RETRIES):
                    try:
                        await page.wait_for_selector(
                            'tbody.text-center.text_vert_midd tr[role="row"]',
                            state="attached",
                            timeout=10000,
                        )
                    except PlaywrightTimeoutError:
                        logging.warning(
                            f"⚠️ 페이지 {page_num} 로딩 시간 초과. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue
                    current_page_data, stop_crawling, error_occurred = (
                        await extract_data_from_page(page, yesterday_str_for_compare)
                    )
                    if error_occurred:
                        logging.warning(
                            f"⚠️ 페이지 {page_num}에서 데이터 추출 오류. {attempt + 1}/{MAX_RETRIES}번째 재시도..."
                        )
                        await page.reload()
                        continue
                    success_on_page = True
                    break
                if not success_on_page:
                    logging.error(
                        f"❌ 페이지 {page_num} 수집에 {MAX_RETRIES}번 실패하여 중단"
                    )
                    stop_crawling = True
                if success_on_page and current_page_data:
                    all_car_data.extend(current_page_data)
                    logging.info(
                        f"✔️ {len(current_page_data)}개 차량 정보 수집 완료. (총 {len(all_car_data)}개)"
                    )
                elif success_on_page and not stop_crawling:
                    logging.warning(
                        "⚠️ 현재 페이지에서 어제 날짜 데이터를 찾지 못했습니다."
                    )
                if stop_crawling:
                    break
                try:
                    active_page_element = page.locator("ul.pagination li.active a")
                    if not await active_page_element.is_visible(timeout=5000):
                        logging.info("⭐ 페이지네이션을 찾을 수 없어 종료")
                        break
                    current_page_num_text = await active_page_element.inner_text()
                    current_page_num = int(current_page_num_text)
                    next_page_num = current_page_num + 1
                    next_page_button = page.locator(
                        f"//ul[contains(@class, 'pagination')]//a[text()='{next_page_num}']"
                    )
                    if await next_page_button.is_visible():
                        logging.info(f"✔️ {next_page_num} 페이지로 이동")
                        await next_page_button.click()
                    else:
                        next_block_button = page.locator(
                            "//ul[contains(@class, 'pagination')]//a[text()='>']"
                        )
                        if await next_block_button.is_visible():
                            logging.info("✔️ 다음 페이지 블록으로 이동")
                            await next_block_button.click()
                        else:
                            logging.info("⭐ 마지막 페이지 도달")
                            break
                    await page.wait_for_selector(
                        'tbody.text-center.text_vert_midd tr[role="row"]',
                        state="attached",
                        timeout=30000,
                    )
                    page_num += 1
                except (PlaywrightError, ValueError) as e:
                    logging.error(f"❌ 페이지 이동/번호 확인 오류: {e}")
                    break
    except Exception as e:
        logging.critical(f"🚨 예기치 않은 오류: {e}", exc_info=True)
    finally:
        if browser:
            await browser.close()
            logging.info("✔️ 브라우저 종료")
        if not all_car_data:
            logging.warning("❌ 수집된 데이터가 없어 파일 저장 생략")
        else:
            logging.info(f"💾 총 {len(all_car_data)}개 저장 및 업로드")
            try:
                local_file_name = f"{SITE}-{yesterday_file}-raw.csv"
                fieldnames = all_car_data[0].keys()
                with open(
                    local_file_name, "w", encoding="utf-8-sig", newline=""
                ) as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_car_data)
                logging.info(f"✔️ 로컬 저장: {local_file_name}")
                s3_key = (
                    f"raw/{SITE}/{yesterday_folder}/{SITE}-{yesterday_file}-raw.csv"
                )
                logging.info(f"S3 업로드 시작: s3://{BUCKET}/{s3_key}")
                boto3.client("s3").upload_file(local_file_name, BUCKET, s3_key)
                logging.info(f"✔️ 업로드 완료: s3://{BUCKET}/{s3_key}")
            except Exception as e:
                logging.error(f"❌ CSV 저장/업로드 오류: {e}", exc_info=True)
            finally:
                if local_file_name and os.path.exists(local_file_name):
                    os.remove(local_file_name)
                    logging.info(f"✔️ 임시 파일 삭제: {local_file_name}")
        # --- 로그 업로드 ---
        try:
            log_s3_key = f"{LOG_PREFIX}/{yesterday_folder}/crawl_{yesterday_file}.log"
            boto3.client("s3").upload_file(LOG_FILE, BUCKET, log_s3_key)
            logging.info(f"✔️ 로그 업로드 완료: s3://{BUCKET}/{log_s3_key}")
        except Exception as e:
            logging.error(f"로그 업로드 실패: {e}")


if __name__ == "__main__":
    asyncio.run(main())
