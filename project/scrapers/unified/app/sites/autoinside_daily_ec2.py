import asyncio
import csv
import io
import random
import re
import signal
import os
from datetime import datetime, date, timedelta, timezone
from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Error as PlaywrightError,
)
from tqdm import tqdm
import boto3
import logging
from logging import handlers

# === 기본 설정 ===
SITE = "autoinside"
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

BASE_LIST_URL = "https://auction.autoinside.co.kr/auction/auction_car_end_list.do"
DETAIL_PAGE_URL_TEMPLATE = (
    "https://auction.autoinside.co.kr/auction/auction_car_view.do?i_sEntryCd={entry_cd}"
)
S3_BUCKET_NAME = BUCKET
CONCURRENT_REQUESTS = 2
MAX_RETRIES = 3

import re as _re


def clean_number(text):
    return int(_re.sub(r"[^0-9]", "", text)) if text else 0


def parse_date(text):
    if not text:
        return "N/A"
    parts = _re.findall(r"\d+", text)
    if len(parts) == 3:
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    return text


async def get_car_detail(page, entry_cd):
    detail_url = DETAIL_PAGE_URL_TEMPLATE.format(entry_cd=entry_cd)
    for attempt in range(MAX_RETRIES):
        try:
            if page.is_closed():
                return None
            await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
            break
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** (attempt + 1))
            else:
                print(f"  - 상세 페이지 로딩 최종 실패: {entry_cd} ({e})")
                return None
    if page.is_closed():
        return None
    car_data = {"entry_cd": entry_cd}
    try:
        car_name_part1 = await page.locator(
            ".performance_info .car_nm .txt01"
        ).inner_text()
        car_name_part2 = await page.locator(
            ".performance_info .car_nm .txt02"
        ).inner_text()
        full_car_name = f"{car_name_part1} {car_name_part2}"
        parts = full_car_name.split(" ", 1)
        if len(parts) > 1:
            car_data["브랜드"] = parts[0]
            car_data["차량명"] = parts[1]
        else:
            car_data["브랜드"] = full_car_name
            car_data["차량명"] = full_car_name
    except Exception:
        car_data["차량명"] = "N/A"
        car_data["브랜드"] = "N/A"
    try:
        car_data["차량번호"] = (
            await page.locator(".fixed_detail_bid_box .car_number").first.inner_text()
        ).strip()
    except Exception:
        car_data["차량번호"] = "N/A"
    try:
        info_list = await page.locator(
            ".performance_info .info_list span"
        ).all_inner_texts()
        car_data["연식"] = clean_number(info_list[1]) if len(info_list) > 1 else 0
        car_data["주행거리"] = clean_number(info_list[2]) if len(info_list) > 2 else 0
        car_data["보관센터"] = info_list[3].strip() if len(info_list) > 3 else "N/A"
    except Exception:
        car_data.update({"연식": 0, "주행거리": 0, "보관센터": "N/A"})
    try:
        car_data["차량등급"] = (
            await page.locator("a.grade.popOpen .txt").inner_text()
        ).strip()
    except Exception:
        car_data["차량등급"] = "N/A"
    try:
        info_table = page.locator(".section_car_info .info_table")
        raw_date = (
            await info_table.locator(
                ".tr:nth-child(1) .td:nth-child(2) .txt"
            ).inner_text()
        ).strip()
        car_data["최초등록일"] = parse_date(raw_date)
        car_data["사고유무"] = (
            await info_table.locator(
                ".tr:nth-child(3) .td:nth-child(1) .txt"
            ).inner_text()
        ).strip()
        car_data["색상"] = (
            await info_table.locator(
                ".tr:nth-child(2) .td:nth-child(2) .txt"
            ).inner_text()
        ).strip()
        fuel_trans = (
            await info_table.locator(
                ".tr:nth-child(2) .td:nth-child(3) .txt"
            ).inner_text()
        ).strip()
        if "/" in fuel_trans:
            fuel, trans = fuel_trans.split("/")
            car_data["연료"] = fuel.strip()
            car_data["변속기"] = trans.strip()
        else:
            car_data["변속기"] = fuel_trans
            car_data["연료"] = "N/A"
        car_data["배기량"] = clean_number(
            await info_table.locator(
                ".tr:nth-child(3) .td:nth-child(2) .txt"
            ).inner_text()
        )
    except Exception:
        car_data.update(
            {
                "최초등록일": "N/A",
                "사고유무": "N/A",
                "색상": "N/A",
                "연료": "N/A",
                "변속기": "N/A",
                "배기량": 0,
            }
        )
    try:
        boxes = page.locator(".info_box02 .box")
        for i in range(await boxes.count()):
            title = (await boxes.nth(i).locator(".tit").inner_text()).strip()
            value = (await boxes.nth(i).locator(".txt").inner_text()).strip()
            car_data[f"성능_{title}"] = value
    except Exception:
        pass
    try:
        acc_items = page.locator(".acc_list .box")
        for i in range(await acc_items.count()):
            item = acc_items.nth(i)
            title = (await item.locator(".tit").inner_text()).strip()
            con = (await item.locator(".con .txt").inner_text()).strip()
            sub = ""
            if await item.locator(".con .sub").count() > 0:
                sub = (await item.locator(".con .sub").inner_text()).strip()
            car_data[f"사고_{title}"] = f"{con} {sub}".strip()
    except Exception:
        pass
    try:
        bid_box = page.locator(".detail_bid_box")
        car_data["경매상태"] = (
            await bid_box.locator(".set_count > .txt:visible").first.inner_text()
        ).strip()
        raw_price = (await bid_box.locator(".bidding_count").inner_text()).strip()
        clean_price = raw_price.replace("*", "0").replace(",", "")
        match = _re.search(r"(\d+)만원", clean_price)
        car_data["낙찰가"] = int(match.group(1)) * 10000 if match else 0
    except Exception:
        car_data.update({"경매상태": "N/A", "낙찰가": 0})
    try:
        announce_text = (
            await page.locator(".detail_bid_box .announce").inner_text()
        ).strip()
        match = _re.search(r"(\d+)월 (\d+)일", announce_text)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            current = yesterday_dt
            year = current.year
            if current.month == 1 and month == 12:
                year -= 1
            car_data["경매종료일"] = f"{year}-{month:02d}-{day:02d}"
        else:
            car_data["경매종료일"] = "N/A"
    except Exception:
        car_data["경매종료일"] = "N/A"
    return car_data


async def fetch_ids_from_page(page, page_num: int):
    list_page_url = f"{BASE_LIST_URL}?i_iNowPageNo={page_num}&sort=A.D_REG_DTM%20DESC"
    entry_cds = []
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(list_page_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_selector(
                ".car_list_box .list li:first-child", timeout=20000
            )
            links = await page.locator(".car_list_box .list li a.a_detail").all()
            for link in links:
                entry_cd = await link.get_attribute("data-entrycd")
                if entry_cd:
                    entry_cds.append(entry_cd)
            return entry_cds
        except Exception as e:
            if attempt >= MAX_RETRIES - 1:
                print(f"  - ID 수집 최종 실패: {page_num} 페이지 ({e})")
                return []
            else:
                await asyncio.sleep(random.uniform(1.5, 3.5))
    return []


async def fetch_car_details_concurrently(
    context: BrowserContext, entry_cd: str, semaphore: asyncio.Semaphore
):
    async with semaphore:
        page = await context.new_page()
        try:
            return await get_car_detail(page, entry_cd)
        finally:
            if not page.is_closed():
                await page.close()
            await asyncio.sleep(random.uniform(0.5, 1.5))


all_car_data_global = []
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    print("\nCtrl+C 감지! 현재까지 수집된 데이터 저장 후 종료합니다...")
    shutdown_event.set()


from signal import SIGINT
import signal as _signal


async def save_data_to_s3(data, target_date):
    if not data:
        print("S3에 업로드할 데이터가 없습니다.")
        return
    data.sort(key=lambda x: x.get("entry_cd", ""), reverse=True)
    folder_date = target_date.strftime("%Y-%m-%d")
    file_date = target_date.strftime("%Y%m%d")
    s3_key = f"raw/{SITE}/{folder_date}/{SITE}-{file_date}-raw.csv"
    print(f"\n수집된 {len(data)}개 -> s3://{S3_BUCKET_NAME}/{s3_key}")
    csv_buffer = io.StringIO()
    fieldnames = [
        "entry_cd",
        "차량번호",
        "브랜드",
        "차량명",
        "차량등급",
        "색상",
        "연료",
        "배기량",
        "변속기",
        "연식",
        "최초등록일",
        "주행거리",
        "성능_엔진",
        "성능_미션",
        "성능_동력/전기계통",
        "성능_내/외관",
        "성능_사제품목",
        "사고유무",
        "사고_내차피해",
        "사고_상대차피해",
        "사고_전손보험사고",
        "사고_침수보험사고",
        "사고_도난보험사고",
        "사고_소유자 변경",
        "사고_차량번호 변경",
        "낙찰가",
        "경매상태",
        "경매종료일",
        "보관센터",
    ]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(data)
    s3 = boto3.client("s3")
    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8-sig"),
        )
        print("S3 업로드 완료!")
    except Exception as e:
        print(f"S3 업로드 오류: {e}")


async def main():
    _signal.signal(SIGINT, signal_handler)
    yesterday = yesterday_dt.date()
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    print(f"어제 날짜({yesterday_str}) 경매 종료 차량 정보를 수집합니다.")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        list_page = None
        all_tasks = set()
        try:
            list_page = await context.new_page()
            global all_car_data_global
            stop_scraping = False
            page_num = 1
            while not stop_scraping and not shutdown_event.is_set():
                print(f"\n--- {page_num} 페이지의 차량 ID 수집 시작 ---")
                entry_cds_on_page = await fetch_ids_from_page(list_page, page_num)
                if not entry_cds_on_page:
                    print("더 이상 차량 정보가 없어 종료합니다.")
                    break
                print(f"{len(entry_cds_on_page)}개의 ID 수집 완료. 상세 정보 확인 시작")
                semaphore = asyncio.Semaphore(2)
                tasks = {
                    asyncio.create_task(
                        fetch_car_details_concurrently(context, entry_cd, semaphore)
                    )
                    for entry_cd in entry_cds_on_page
                }
                all_tasks.update(tasks)
                from tqdm import tqdm as _tqdm

                try:
                    for future in _tqdm(
                        asyncio.as_completed(tasks),
                        total=len(tasks),
                        desc=f"페이지 {page_num} 상세 정보 수집 중",
                    ):
                        if shutdown_event.is_set():
                            stop_scraping = True
                            break
                        try:
                            car_data = await future
                        except (asyncio.CancelledError, PlaywrightError):
                            continue
                        if not car_data or "경매종료일" not in car_data:
                            continue
                        end_date_str = car_data.get("경매종료일", "N/A")
                        if end_date_str == yesterday_str:
                            all_car_data_global.append(car_data)
                        elif end_date_str < yesterday_str and end_date_str != "N/A":
                            print(f"어제 이전 날짜({end_date_str}) 발견 → 중단")
                            stop_scraping = True
                            break
                finally:
                    remaining_tasks = [t for t in tasks if not t.done()]
                    if remaining_tasks:
                        print(
                            f"\n현재 페이지의 남은 작업 {len(remaining_tasks)}개 취소"
                        )
                        for t in remaining_tasks:
                            t.cancel()
                        await asyncio.gather(*remaining_tasks, return_exceptions=True)
                all_tasks.difference_update(tasks)
                if stop_scraping:
                    break
                page_num += 1
        except Exception as e:
            print(f"크롤링 중 에러: {e}")
        finally:
            print("\n마무리 작업 시작...")
            if all_tasks:
                print(f"전체 남은 작업 {len(all_tasks)}개 취소")
                for t in all_tasks:
                    t.cancel()
                await asyncio.gather(*all_tasks, return_exceptions=True)
            await save_data_to_s3(all_car_data_global, yesterday)
            if list_page and not list_page.is_closed():
                await list_page.close()
            if context:
                await context.close()
            if browser:
                await browser.close()
            print("브라우저 종료")
    # --- 로그 업로드 ---
    try:
        log_s3_key = f"{LOG_PREFIX}/{yesterday_folder}/crawl_{yesterday_file}.log"
        boto3.client("s3").upload_file(LOG_FILE, BUCKET, log_s3_key)
        logging.info(f"✔️ 로그 업로드 완료: s3://{BUCKET}/{log_s3_key}")
    except Exception as e:
        logging.error(f"로그 업로드 실패: {e}")


if __name__ == "__main__":
    asyncio.run(main())
