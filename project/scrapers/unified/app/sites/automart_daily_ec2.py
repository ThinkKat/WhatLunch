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
import bs4
import logging

# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- 상수 설정 ---
BASE_URL = "https://www.automart.co.kr"
AUCTION_LIST_URL = f"{BASE_URL}/views/pub_auction/pub_auction_intro.asp?num=4"
CAR_CHECK_PAPER_URL = f"{BASE_URL}/views/pub_auction/Common/GmSpec_Report_us.asp"
S3_BUCKET = os.environ.get("BUCKET", "whatlunch-s3")
REAL_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

# --- 데이터 파싱 함수 (기존 코드 활용) ---


def parse_checkpaper(html_text: str) -> dict:
    soup = bs4.BeautifulSoup(html_text, "html.parser")
    result = {}
    basic_info = {}
    basic_table = soup.select_one("table.table-striped")
    if basic_table:
        tds = [td.get_text(strip=True) for td in basic_table.find_all("td")]
        for i in range(0, len(tds), 2):
            key, val = tds[i], tds[i + 1] if i + 1 < len(tds) else ""
            basic_info[key.replace(" ", "")] = val if val else "NULL"
    result["차량기본정보"] = basic_info

    special_notes, extracted, accident_info = {}, {}, {}
    for title in ["특이사항", "외장, 내장 종합 소견", "외장교환, 수리요망부위"]:
        cell = soup.find("td", string=lambda x: x and title in x)
        if cell:
            val = cell.find_parent("tr").find_next_sibling("tr").get_text(strip=True)
            special_notes[title] = val if val else "NULL"
            if title == "특이사항" and val:
                m = re.search(r"최초 ?등록일\((\d{4}-\d{2}-\d{2})\)", val)
                if m:
                    extracted["최초등록일"] = m.group(1)
    result["특이사항및소견"] = special_notes
    result["특이사항추출"] = extracted
    return result


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
    soup = bs4.BeautifulSoup(car_info["car_info"], "html.parser")
    car_info_table = soup.find(class_="car_info")
    auction_period_text = soup.find("td", class_="car_title").text

    match_period = re.search(
        r"입찰신청 기간\s*:\s*(\d{4}년 \d{2}월 \d{2}일)", auction_period_text
    )
    match_result = re.search(
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


# --- 메인 크롤링 로직 ---


async def fetch_car_details(page, car_info_url):
    """개별 차량 상세 정보 및 점검표 HTML을 가져옵니다."""
    try:
        await page.goto(car_info_url, wait_until="domcontentloaded", timeout=30000)
        car_html = await page.content()
        soup = bs4.BeautifulSoup(car_html, "html.parser")

        car_pic_link = soup.select_one("td.car_pic a")
        if not car_pic_link:
            return None, None

        href = car_pic_link["href"]
        params_str = href.split(",")[3].replace("'", "").replace(")", "")

        # URL 쿼리 파라미터 파싱
        params = dict(p.split("=") for p in params_str.split("&"))

        check_paper_url = f"{CAR_CHECK_PAPER_URL}?chargecd={params.get('chargecd')}&cifyear={params.get('cifyear')}&cifseqno={params.get('cifseqno')}&carno={params.get('carno')}"

        await page.goto(check_paper_url, wait_until="domcontentloaded", timeout=30000)
        car_checkpaper_html = await page.content()

        return car_html, car_checkpaper_html
    except (PlaywrightError, AttributeError) as e:
        logging.warning(f"차량 상세 정보 수집 실패: {car_info_url}, 오류: {e}")
        return None, None


async def main():
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    logging.info(f"오토마트 어제({yesterday_str})자 경매 데이터 수집을 시작합니다.")

    all_car_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=REAL_UA)
        page = await context.new_page()

        try:
            # 1. 어제 날짜의 경매 공고 목록 찾기
            await page.goto(AUCTION_LIST_URL, wait_until="domcontentloaded")

            rows = await page.locator("table.tb1_1 tbody tr").all()
            target_auction_links = []
            for row in rows[1:]:  # 헤더 제외
                cols = await row.locator("td").all()
                if len(cols) > 2 and yesterday_str in await cols[1].inner_text():
                    link = await cols[0].locator("a").get_attribute("href")
                    target_auction_links.append(f"{BASE_URL}{link}")

            logging.info(f"총 {len(target_auction_links)}개의 경매 공고를 찾았습니다.")

            # 2. 각 경매 공고에서 차량 정보 수집
            for auction_url in target_auction_links:
                await page.goto(auction_url, wait_until="domcontentloaded")

                # '입찰결과' 탭으로 이동
                result_tab = page.locator("div.tabmenu li a:has-text('입찰결과')")
                if await result_tab.count() > 0:
                    await result_tab.click()
                    await page.wait_for_load_state("domcontentloaded")

                car_rows = await page.locator("table.board_list_search_content").all()
                logging.info(
                    f"'{auction_url}' 에서 {len(car_rows)}대의 차량을 발견했습니다."
                )

                for car_row in car_rows:
                    car_link_tag = car_row.locator("td.serach_mina_text a")
                    if await car_link_tag.count() == 0:
                        continue

                    car_info_url_relative = await car_link_tag.get_attribute("href")
                    car_info_url = f"{BASE_URL}{car_info_url_relative}"

                    tds = await car_row.locator("td").all()
                    winning_price = await tds[4].inner_text() if len(tds) > 4 else "0"
                    no_participants = await tds[5].inner_text() if len(tds) > 5 else "0"

                    # 3. 개별 차량 상세 정보 수집
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
                        processed_data = get_car_info_data(raw_info)
                        all_car_data.append(processed_data)

        except Exception as e:
            logging.critical(f"스크립트 실행 중 오류 발생: {e}", exc_info=True)
        finally:
            await browser.close()

    # 4. S3에 업로드
    if not all_car_data:
        logging.warning("수집된 데이터가 없습니다. S3 업로드를 건너뜁니다.")
        return

    logging.info(f"총 {len(all_car_data)}개의 차량 데이터를 S3에 업로드합니다.")
    local_file_name = f"automart-{yesterday.strftime('%Y%m%d')}-raw.csv"

    try:
        # 모든 키를 모아 헤더 생성
        headers = sorted(list(set(key for car in all_car_data for key in car.keys())))

        with open(local_file_name, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_car_data)

        s3_client = boto3.client("s3")
        s3_key = f"raw/automart/{yesterday_str}/{local_file_name}"
        s3_client.upload_file(local_file_name, S3_BUCKET, s3_key)
        logging.info(f"S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")

    except Exception as e:
        logging.error(f"파일 저장 또는 S3 업로드 중 오류 발생: {e}", exc_info=True)
    finally:
        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            logging.info(f"로컬 임시 파일 '{local_file_name}'을 삭제했습니다.")


if __name__ == "__main__":
    asyncio.run(main())
