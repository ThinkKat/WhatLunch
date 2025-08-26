import sys
import re
import time
import csv
import json
from datetime import datetime
import asyncio

from playwright.async_api import async_playwright
from playwright.async_api import Error, TimeoutError
import bs4 

BASE = "https://www.automart.co.kr"
CAR_CHECK_PAPER_URL = "https://www.automart.co.kr/views/pub_auction/Common/GmSpec_Report_us.asp"
CAR_CHECK_IMAGE_URL = "https://www.automart.co.kr/views/pub_auction/Common/ImageView.asp"

# 리눅스(EC2) 환경에 맞는 UA
REAL_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36"
)

# 현실적인 기본 헤더
EXTRA_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Upgrade-Insecure-Requests": "1",
    # 필요 시 켜기 (목록→상세 플로우를 흉내낼 때 유용)
    # "Referer": "https://www.automart.co.kr/",
    # UA-CH를 리눅스 쪽으로 일관되게 맞추고 싶다면:
    "sec-ch-ua": '"Chromium";v="139", "Google Chrome";v="139", "Not=A?Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}

async def make_context(p, *, headless=True, reuse_state=True, state_path="state.json"):
    browser = await p.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--lang=ko-KR",
        ],
    )

    context = await browser.new_context(
        user_agent=REAL_UA,
        locale="ko-KR",
        timezone_id="Asia/Seoul",
        java_script_enabled=True,
        viewport={"width": 1366, "height": 768}
    )

    # 전역 타임아웃
    context.set_default_navigation_timeout(60_000)
    context.set_default_timeout(60_000)

    # 추가 헤더 적용
    await context.set_extra_http_headers(EXTRA_HEADERS)

    # 불필요 리소스 절식(선택): 이미지/폰트/미디어/애널리틱스 차단
    await context.route("**/*", lambda route, req: (
        route.abort() if req.resource_type in {"image", "font", "media"}
        or any(h in req.url for h in ["googletagmanager.com","google-analytics.com"])
        else route.continue_()
    ))

    # 헤드리스 티 약화
    await context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

    return browser, context

def parse_checkpaper(html_text: str) -> dict:
    soup = bs4.BeautifulSoup(html_text, "html.parser")
    result = {}

    # --- 차량 기본 정보 ---
    basic_info = {}
    basic_table = soup.select_one("table.table-striped")
    if basic_table:
        tds = [td.get_text(strip=True) for td in basic_table.find_all("td")]
        for i in range(0, len(tds), 2):
            key, val = tds[i], tds[i+1] if i+1 < len(tds) else ""
            basic_info[key.replace(" ", "")] = val if val else "NULL"
    result["차량기본정보"] = basic_info

    # --- 특이사항 / 종합소견 / 수리요망부위 ---
    special_notes = {}
    extracted = {}
    accident_info = {}

    for title in ["특이사항", "외장, 내장 종합 소견", "외장교환, 수리요망부위"]:
        cell = soup.find("td", string=lambda x: x and title in x)
        if cell:
            val = cell.find_parent("tr").find_next_sibling("tr").get_text(strip=True)
            special_notes[title] = val if val else "NULL"
            # 특이사항 추가 추출
            if title == "특이사항" and val:
                m = re.search(r"제작연도\((\d{4})\)", val)
                if m:
                    extracted["제작연도"] = m.group(1)
                m = re.search(r"최초 ?등록일\((\d{4}-\d{2}-\d{2})\)", val)
                if m:
                    extracted["최초등록일"] = m.group(1)
                m = re.search(r"검사 ?유효기간\((\d{4}-\d{2}-\d{2})\)", val)
                if m:
                    extracted["검사유효기간"] = m.group(1)

            # 사고이력 파싱
            if val and "사고이력" in val:
                # 내차 피해
                m = re.search(r"내차 ?피해 ?([^\),]+)", val)
                if m:
                    accident_info["내차피해"] = m.group(1).strip()
                # 상대차 피해
                m = re.search(r"상대차 ?피해 ?([^\),]+)", val)
                if m:
                    accident_info["상대차피해"] = m.group(1).strip()

    result["특이사항및소견"] = special_notes
    result["특이사항추출"] = extracted
    result["사고이력"] = accident_info if accident_info else {}

    return result

WANTED_KEYS = [
    "차량순번","차량번호","차량명","모델번호/기어","주행거리",
    "공고","예정가","색상/배기량","보관소","유의사항","차량설명","말소등록비"
]

# 타이틀 정규화(표제 텍스트 → 최종 키)
def normalize_title(title: str) -> str:
    t = " ".join(title.split())  # 공백 정리
    # 사이트 표기 ↔︎ 요청 키 매핑
    if t == "모델연도/기어":
        return "모델번호/기어"
    if t == "예 정 가":
        return "예정가"
    if t.startswith("차량설명"):
        return "차량설명"
    if t.startswith("말소등록비"):
        return "말소등록비"
    if t in WANTED_KEYS:
        return t
    return t  # 혹시 모를 예외 대비(나중 필터링됨)

def extract_text(td) -> str:
    # 내부 div/br/span 등 모두 평탄화
    return " ".join(td.get_text(separator=" ", strip=True).split())

def parse_detail_table(table) -> dict:

    result = {k: None for k in WANTED_KEYS}

    # 각 행을 돌며 tit_blue → tb_cont 페어를 수집
    for tr in table.select("tr"):
        tds = tr.find_all("td", recursive=False)
        i = 0
        while i < len(tds):
            td = tds[i]
            classes = td.get("class", [])
            if "tit_blue" in classes:
                # 같은 행에서 뒤쪽의 첫 tb_cont를 값으로 매칭
                val_td = None
                for j in range(i + 1, len(tds)):
                    if "tb_cont" in (tds[j].get("class") or []):
                        val_td = tds[j]
                        i = j  # 점프
                        break
                if val_td:
                    key = normalize_title(extract_text(td))
                    val = extract_text(val_td)
                    if key in WANTED_KEYS:
                        result[key] = val
            i += 1
    return result

async def fetch_html(url: str):
    """
    url: 공고 url
    OUTPUT_PATH: html 파일 저장하는 곳

    return:
        [
            car_no: {
                "winning_price" : ,
                "no_participants" : ,
                "car_info": 차량 상세 html,
                "car_checkpaper": 차량 점검서 html
            }
        ]
        
    """

    results = {"auction_notice": None, "car_info": {}}

    playwright = await async_playwright().start()
    browser, context = await make_context(playwright)
    # browser = await playwright.chromium.launch(headless=True)
    tab = await context.new_page()
    tab.set_default_timeout(5000)

    # 공고 페이지
    cnt = 0
    while True:
        try:
            await tab.goto(url, wait_until="domcontentloaded")
        except TimeoutError:
            print("Retry after timeout")
            continue
        except Error:
            print("Retry due to some errors")
            continue
            
        time.sleep(0.5)
        try:
            html = await tab.inner_html("body")
        except TimeoutError:
            print("Retry after timeout")
            continue
        except Error:
            print("Retry due to some errors")
            continue
        soup = bs4.BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        cnt += 1
        print(f"Try: {cnt}")
        if len(tables) >= 20:
            break

    # auction_name = soup.find("td", class_="td_title_c").text.strip().replace(" ", "")
    # auntion_cd = [v.split("=")[1] for v in url.rsplit("/", 1)[1].split("&") if v.startswith("NotNo")][0]

    # 저장
    # auction_file = f"{OUTPUT_PATH}/{auction_name.split('[', 1)[0]}_{auntion_cd}_{dt}.html"
    # with open(auction_file, "w") as f:
    #     f.write(html)

    results = []
    # 차량 리스트
    # print(soup.find_all("table"))
    car_list = tables[20]
    car_info_list = []
    for row in car_list.find_all("tr")[1:]:
        data = row.find_all("td")
        if data and len(data) >= 7:
            elem = data[1].find("a")
            if elem:
                car_info_url = elem.attrs["href"].strip()
                car_info_url = f"{BASE}{car_info_url}" if car_info_url.startswith("/") else f"{url.rsplit('/', 1)[0]}/{car_info_url}"

                car_info = {
                    "winning_price": data[4].text,
                    "no_participants": data[5].text,
                    "url": car_info_url
                }
                car_info_list.append(car_info)

    # 각 차량 정보
    for car_info in car_info_list:
        # 차량 상세
        while True:
            try:
                await tab.goto(car_info["url"], wait_until="domcontentloaded")
            except TimeoutError:
                print("Retry after timeout")
                continue
            except Error:
                print("Retry due to some errors")
                continue
            time.sleep(0.5)

            print("Succes to load car info page")
            try:
                car_html = await tab.inner_html("body") 
            except TimeoutError:
                print("Retry after timeout")
                continue
            except Error:
                print("Retry due to some errors")
                continue

            soup = bs4.BeautifulSoup(car_html, "html.parser")
            car_pictures = soup.find("td", class_="car_pic")
            if car_pictures: break

        car_code = [
            variable.split("=")
            for variable in [img.attrs["href"] for img in car_pictures.find_all("a")][0]
            .split(",")[3].replace("'", "").replace(")", "").split("&")
        ]
        used_keys = ["chargecd", "cifyear", "cifseqno", "carno"]
        car_code_url = "&".join(["=".join(code) for code in car_code if code[0] in used_keys])
        car_check_paper_url = f"{CAR_CHECK_PAPER_URL}?{car_code_url}"

        # 차량 점검서
        while True:
            try:
                await tab.goto(car_check_paper_url, wait_until="domcontentloaded")
            except TimeoutError:
                print("Retry after timeout")
                continue
            except Error:
                print("Retry due to some errors")
                continue

            time.sleep(0.5)
            try:
                car_checkpaper_html = await tab.inner_html("body")
                break
            except TimeoutError:
                print("Retry after timeout")
                continue
            except Error:
                print("Retry due to some errors")
                continue
            
        # with open(f"{OUTPUT_PATH}/{auction_name.split('[', 1)[0]}_{auntion_cd}_{car_no}_checkpaper_{dt}.html", "w") as f:
        #     f.write(car_checkpaper_html)

        # 결과 dict에 추가
        results.append({
            "winning_price": car_info["winning_price"],
            "no_participants": car_info["no_participants"],
            "car_info": car_html,
            "car_checkpaper": car_checkpaper_html
        })
        

    await browser.close()
    return results


def get_car_info_data(car_info: dict):
    soup = bs4.BeautifulSoup(car_info["car_info"], "html.parser")
        
    car_info_table = soup.find(class_="car_info")

    auction_period = soup.find("td", class_="car_title")
    text = auction_period.text

    # 입찰신청 기간
    match_period = re.search(r"입찰신청 기간\s*:\s*(\d{4}년 \d{2}월 \d{2}일)\s*~\s*(\d{4}년 \d{2}월 \d{2}일) \d{2}시 \d{2}분", text)
    # 발표일시
    match_result = re.search(r"발표일시\s*:\s*(\d{4}년 \d{2}월 \d{2}일) \d{2}시 \d{2}분|발표일시\s*:\s*(마감.*)", text)
    
    # information
    data = parse_detail_table(car_info_table)
    data["입찰시작일자"] = datetime.strptime(match_period.group(1), "%Y년 %m월 %d일").strftime("%Y-%m-%d")
    data["입찰종료일자"] = datetime.strptime(match_period.group(2), "%Y년 %m월 %d일").strftime("%Y-%m-%d")
    if match_result.group(1) is not None:
        data["경매발표일자"] = datetime.strptime(match_result.group(1), "%Y년 %m월 %d일").strftime("%Y-%m-%d")
    elif match_result.group(2) is not None:
        data["경매발표일자"] = match_result.group(2)
    else:
        data["경매발표일자"] = None
        
    paper_result = parse_checkpaper(car_info["car_checkpaper"])

    # 차량기본정보
    if "제조사" in paper_result["차량기본정보"]:
        data["제조사"] = paper_result["차량기본정보"]["제조사"]
    if "주행거리(km)" in paper_result["차량기본정보"]:
        data["주행거리"] = paper_result["차량기본정보"]["주행거리(km)"]
    if "변속기" in paper_result["차량기본정보"]:
        data["변속기"] = paper_result["차량기본정보"]["변속기"]
    if "원동기형식" in paper_result["차량기본정보"]:
        data["원동기형식"] = paper_result["차량기본정보"]["원동기형식"]
    if "구동방식" in paper_result["차량기본정보"]:
        data["구동방식"] = paper_result["차량기본정보"]["구동방식"]
    if "색상" in paper_result["차량기본정보"]:
        data["색상"] = paper_result["차량기본정보"]["색상"]
    if "차대번호" in paper_result["차량기본정보"]:
        data["차대번호"] = paper_result["차량기본정보"]["차대번호"]
    if "타입" in paper_result["차량기본정보"]:
        data["타입"] = paper_result["차량기본정보"]["타입"]
    if "연료" in paper_result["차량기본정보"]:
        data["연료"] = paper_result["차량기본정보"]["연료"]
    if "최초등록일" in paper_result["특이사항추출"]:
        data["최초등록일"] = paper_result["특이사항추출"]["최초등록일"]
            
    data["winning_price"] = car_info["winning_price"]

    return data

if __name__ == "__main__":
    async def main():
        argv = sys.argv

        for i, arg in enumerate(argv):
            if arg == "--date":
                date = argv[i+1]

        print("------------Scarping Start------------")
        # print(f"url:{url} \ndate:{date}\n")
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%s')}")

        count = 0
        with open("automart/daily/closed/auction_url_check_list.json", "r") as f:
            auction_url_check_list = json.load(f)
        
        car_info_list = []
        for auction_url in auction_url_check_list:
            if auction_url_check_list[auction_url] == 1:
                continue

            start = time.time()
            car_info = await fetch_html(auction_url)
            car_info_list.extend(car_info)
            print(auction_url, f"{time.time() - start:.2f}...")
            count+=1
            auction_url_check_list[auction_url] = 1
            if count > 5:
                break

        total_data = []
        print(len(car_info_list))
        for car_info in car_info_list:
            data = get_car_info_data(car_info)
            total_data.append(data)

        with open(f"automart/daily/closed/complete_data_{date}.json", "r", encoding="utf-8") as f:
            prev_total_data = json.load(f)
            total_data.extend(prev_total_data)
            
        with open(f"automart/daily/closed/complete_data_{date}.json", "w", encoding="utf-8") as f:
            json.dump(total_data, f, ensure_ascii=False, indent=2)

            # 저장 성공한 경우에만
            with open("automart/daily/closed/auction_url_check_list.json", "w", encoding="utf-8") as f:
                json.dump(auction_url_check_list, f, ensure_ascii=False, indent=2)

        print(f"End: {datetime.now().strftime('%Y-%m-%d %H:%M:%s')}")

    asyncio.run(main())
