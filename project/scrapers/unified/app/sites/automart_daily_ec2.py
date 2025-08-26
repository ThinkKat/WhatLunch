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
import bs4
import logging
from logging import handlers

# === 설정 ===
SITE = "automart"
BUCKET = os.environ.get("BUCKET", "whatlunch-s3")
LOG_PREFIX = os.environ.get("LOG_PREFIX", f"logs/{SITE}")
LOG_DIR = f"/app/logs/{SITE}"
os.makedirs(LOG_DIR, exist_ok=True)

KST = timezone(timedelta(hours=9))
yesterday_dt = datetime.now(KST) - timedelta(days=1)
yesterday_folder = yesterday_dt.strftime("%Y-%m-%d")
yesterday_file = yesterday_dt.strftime("%Y%m%d")

LOG_FILE = os.path.join(LOG_DIR, f"crawl_{yesterday_file}.log")

# --- 로깅(콘솔+파일) ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
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

# --- 상수 ---
BASE_URL = "https://www.automart.co.kr"
AUCTION_LIST_URL = f"{BASE_URL}/views/pub_auction/pub_auction_intro.asp?num=4"
CAR_CHECK_PAPER_URL = f"{BASE_URL}/views/pub_auction/Common/GmSpec_Report_us.asp"
S3_BUCKET = BUCKET
REAL_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

import bs4 as _bs4


def parse_checkpaper(html_text: str) -> dict:
    soup = _bs4.BeautifulSoup(html_text, "html.parser")
    result = {}
    basic_info = {}
    basic_table = soup.select_one("table.table-striped")
    if basic_table:
        tds = [td.get_text(strip=True) for td in basic_table.find_all("td")]
        for i in range(0, len(tds), 2):
            key, val = tds[i], tds[i + 1] if i + 1 < len(tds) else ""
            basic_info[key.replace(" ", "")] = val if val else "NULL"
    result["차량기본정보"] = basic_info
    import re as _re

    special_notes, extracted = {}, {}
    for title in ["특이사항", "외장, 내장 종합 소견", "외장교환, 수리요망부위"]:
        cell = soup.find("td", string=lambda x: x and title in x)
        if cell:
            val = cell.find_parent("tr").find_next_sibling("tr").get_text(strip=True)
            special_notes[title] = val if val else "NULL"
            if title == "특이사항" and val:
                m = _re.search(r"최초 ?등록일\((\d{4}-\d{2}-\d{2})\)", val)
                if m:
                    extracted["최초등록일"] = m.group(1)
    result["특이사항및소견"] = special_notes
    result["특이사항추출"] = extracted
    return result


from bs4 import BeautifulSoup as _BS


def parse_detail_table(table) -> dict:
    WANTED_KEYS = [
        "차량순번",
        "차량번호",
        "차량명",
        "모델번호/기어",
        "주행거리",
        "공고",
        "예정가",
        "색상/배기량",
        "보관소",
        "유의사항",
        "차량설명",
        "말소등록비",
    ]
    result = {k: None for k in WANTED_KEYS}
    for tr in table.select("tr"):
        tds = tr.find_all("td", recursive=False)
        i = 0
        while i < len(tds):
            td = tds[i]
            if "tit_blue" in td.get("class", []):
                for j in range(i + 1, len(tds)):
                    if "tb_cont" in (tds[j].get("class") or []):
                        val_td = tds[j]
                        key = " ".join(td.get_text(strip=True).split())
                        if key == "모델연도/기어":
                            key = "모델번호/기어"
                        if key in WANTED_KEYS:
                            result[key] = " ".join(
                                val_td.get_text(separator=" ", strip=True).split()
                            )
                        i = j
                        break
            i += 1
    return result


def get_car_info_data(car_info: dict):
    soup = _BS(car_info["car_info"], "html.parser")
    car_info_table = soup.find(class_="car_info")
    auction_period_text = soup.find("td", class_="car_title").text
    import re as _re

    match_period = _re.search(
        r"입찰신청 기간\s*:\s*(\d{4}년 \d{2}월 \d{2}일)", auction_period_text
    )
    match_result = _re.search(
        r"발표일시\s*:\s*(\d{4}년 \d{2}월 \d{2}일)", auction_period_text
    )
    data = parse_detail_table(car_info_table)
    data["입찰시작일자"] = (
        datetime.strptime(match_period.group(1), "%Y년 %m월 %d일").strftime("%Y-%m-%d")
        if match_period
        else None
    )
    data["경매발표일자"] = (
        datetime.strptime(match_result.group(1), "%Y년 %m월 %d일").strftime("%Y-%m-%d")
        if match_result
        else None
    )
    paper_result = parse_checkpaper(car_info["car_checkpaper"])
    basic_info = paper_result.get("차량기본정보", {})
    data["제조사"] = basic_info.get("제조사")
    data["변속기"] = basic_info.get("변속기")
    data["연료"] = basic_info.get("연료")
    data["차대번호"] = basic_info.get("차대번호")
    extracted_info = paper_result.get("특이사항추출", {})
    data["최초등록일"] = extracted_info.get("최초등록일")
    data["낙찰가"] = car_info["winning_price"]
    data["참가수"] = car_info["no_participants"]
    return data


async def fetch_car_details(page, car_info_url):
    try:
        await page.goto(car_info_url, wait_until="domcontentloaded", timeout=30000)
        car_html = await page.content()
        soup = _BS(car_html, "html.parser")
        car_pic_link = soup.select_one("td.car_pic a")
        if not car_pic_link:
            return None, None
        href = car_pic_link["href"]
        params_str = href.split(",")[3].replace("'", "").replace(")", "")
        params = dict(p.split("=") for p in params_str.split("&"))
        check_paper_url = f"{CAR_CHECK_PAPER_URL}?chargecd={params.get('chargecd')}&cifyear={params.get('cifyear')}&cifseqno={params.get('cifseqno')}&carno={params.get('carno')}"
        await page.goto(check_paper_url, wait_until="domcontentloaded", timeout=30000)
        car_checkpaper_html = await page.content()
        return car_html, car_checkpaper_html
    except (PlaywrightError, AttributeError) as e:
        logging.warning(f"차량 상세 수집 실패: {car_info_url}, 오류: {e}")
        return None, None


async def main():
    logging.info(f"오토마트 어제({yesterday_folder})자 경매 데이터 수집 시작")
    all_car_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=REAL_UA)
        page = await context.new_page()
        try:
            await page.goto(AUCTION_LIST_URL, wait_until="domcontentloaded")
            rows = await page.locator("table.tb1_1 tbody tr").all()
            target_auction_links = []
            for row in rows[1:]:
                cols = await row.locator("td").all()
                if len(cols) > 2 and yesterday_folder in await cols[1].inner_text():
                    link = await cols[0].locator("a").get_attribute("href")
                    target_auction_links.append(f"{BASE_URL}{link}")
            logging.info(f"총 {len(target_auction_links)}개 경매 공고")
            for auction_url in target_auction_links:
                await page.goto(auction_url, wait_until="domcontentloaded")
                result_tab = page.locator("div.tabmenu li a:has-text('입찰결과')")
                if await result_tab.count() > 0:
                    await result_tab.click()
                    await page.wait_for_load_state("domcontentloaded")
                car_rows = await page.locator("table.board_list_search_content").all()
                logging.info(f"'{auction_url}' 에서 {len(car_rows)}대 발견")
                for car_row in car_rows:
                    car_link_tag = car_row.locator("td.serach_mina_text a")
                    if await car_link_tag.count() == 0:
                        continue
                    car_info_url_relative = await car_link_tag.get_attribute("href")
                    car_info_url = f"{BASE_URL}{car_info_url_relative}"
                    tds = await car_row.locator("td").all()
                    winning_price = await tds[4].inner_text() if len(tds) > 4 else "0"
                    no_participants = await tds[5].inner_text() if len(tds) > 5 else "0"
                    car_html, car_checkpaper_html = await fetch_car_details(
                        page, car_info_url
                    )
                    if car_html and car_checkpaper_html:
                        raw_info = {
                            "winning_price": winning_price.strip(),
                            "no_participants": no_participants.strip(),
                            "car_info": car_html,
                            "car_checkpaper": car_checkpaper_html,
                        }
                        processed = get_car_info_data(raw_info)
                        all_car_data.append(processed)
        except Exception as e:
            logging.critical(f"스크립트 실행 오류: {e}", exc_info=True)
        finally:
            await browser.close()
    if not all_car_data:
        logging.warning("수집된 데이터 없음. 업로드 생략")
        # 로그 업로드만 수행
        try:
            log_s3_key = f"{LOG_PREFIX}/{yesterday_folder}/crawl_{yesterday_file}.log"
            boto3.client("s3").upload_file(LOG_FILE, S3_BUCKET, log_s3_key)
            logging.info(f"✔️ 로그 업로드 완료: s3://{S3_BUCKET}/{log_s3_key}")
        except Exception as e:
            logging.error(f"로그 업로드 실패: {e}")
        return
    logging.info(f"총 {len(all_car_data)}개 차량 데이터 S3 업로드")
    local_file_name = f"{SITE}-{yesterday_file}-raw.csv"
    try:
        headers = sorted(list(set(k for car in all_car_data for k in car.keys())))
        with open(local_file_name, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_car_data)
        s3_client = boto3.client("s3")
        s3_key = f"raw/{SITE}/{yesterday_folder}/{local_file_name}"
        s3_client.upload_file(local_file_name, S3_BUCKET, s3_key)
        logging.info(f"S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"CSV 저장/업로드 오류: {e}", exc_info=True)
    finally:
        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            logging.info(f"임시 파일 삭제: {local_file_name}")
    # --- 로그 업로드 ---
    try:
        log_s3_key = f"{LOG_PREFIX}/{yesterday_folder}/crawl_{yesterday_file}.log"
        boto3.client("s3").upload_file(LOG_FILE, S3_BUCKET, log_s3_key)
        logging.info(f"✔️ 로그 업로드 완료: s3://{S3_BUCKET}/{log_s3_key}")
    except Exception as e:
        logging.error(f"로그 업로드 실패: {e}")


if __name__ == "__main__":
    asyncio.run(main())
