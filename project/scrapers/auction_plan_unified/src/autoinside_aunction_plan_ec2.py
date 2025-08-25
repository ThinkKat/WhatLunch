import asyncio
import io
import csv
import random
import re
import signal
from datetime import datetime
from playwright.async_api import async_playwright, BrowserContext, Page, TimeoutError
from tqdm.asyncio import tqdm
import boto3

# --- 설정 ---
# 크롤링할 경매 목록 URL (진행중, 대기중)
BASE_URLS_TO_CRAWL = {
    "ongoing": "https://auction.autoinside.co.kr/auction/auction_car_list.do",
    "waiting": "https://auction.autoinside.co.kr/auction/auction_car_wait_list.do",
}
DETAIL_PAGE_URL_TEMPLATE = (
    "https://auction.autoinside.co.kr/auction/auction_car_view.do?i_sEntryCd={entry_cd}"
)
# S3 저장 경로 템플릿
S3_PATH_TEMPLATE = "s3://whatlunch-s3/auction-plan/raw/autoinside/{date_ymd}/autoinside-{date_ymd_plain}-raw.csv"

# 동시 처리 요청 수 (EC2 사양에 따라 1~2로 낮춰야 할 수 있습니다)
CONCURRENT_REQUESTS = 2
# 요청 실패 시 재시도 횟수
MAX_RETRIES = 3


def clean_number(text: str) -> int:
    """텍스트에서 숫자만 추출하여 정수형으로 반환합니다."""
    return int(re.sub(r"[^0-9]", "", text)) if text else 0


def parse_date(text: str) -> str:
    """'YYYY년 MM월 DD일' 또는 'YYYY.MM.DD' 형식의 문자열을 'YYYY-MM-DD'로 변환합니다."""
    if not text:
        return "N/A"
    text = text.replace(".", "-")
    parts = re.findall(r"\d+", text)
    if len(parts) == 3:
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    return text


async def get_car_detail(page: Page, entry_cd: str) -> dict | None:
    """차량 상세 정보 페이지에서 데이터를 추출합니다."""
    detail_url = DETAIL_PAGE_URL_TEMPLATE.format(entry_cd=entry_cd)

    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
            break
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** (attempt + 1)
                print(
                    f"  - 상세 페이지 로딩 재시도 ({attempt+1}/{MAX_RETRIES}): {entry_cd}, {wait_time}초 후..."
                )
                await asyncio.sleep(wait_time)
            else:
                print(f"  - 상세 페이지 로딩 최종 실패: {entry_cd} ({e})")
                return None

    car_data = {"entry_cd": entry_cd}
    try:
        car_name_part1 = await page.locator(
            ".performance_info .car_nm .txt01"
        ).inner_text()
        car_name_part2 = await page.locator(
            ".performance_info .car_nm .txt02"
        ).inner_text()
        full_car_name = f"{car_name_part1} {car_name_part2}".strip()

        if " " in full_car_name:
            brand, car_name = full_car_name.split(" ", 1)
            car_data["브랜드"] = brand
            car_data["차량명"] = car_name.strip()
        else:
            car_data["브랜드"] = full_car_name
            car_data["차량명"] = full_car_name
    except Exception:
        car_data["차량명"] = "N/A"
        car_data["브랜드"] = "N/A"

    # ... (기존 상세 정보 추출 로직은 동일) ...
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
        bid_box = page.locator(".detail_bid_box").first
        start_line = await bid_box.get_attribute("data-startline")
        deadline = await bid_box.get_attribute("data-deadline")
        if start_line and " " in start_line:
            date_part, time_part = start_line.split(" ", 1)
            car_data["경매날짜"] = date_part
            car_data["경매시작시간"] = time_part[:5]
        else:
            announce_text = await page.locator(".announce").first.inner_text()
            date_match = re.search(r"(\d{4}[.]\d{2}[.]\d{2})", announce_text)
            car_data["경매날짜"] = (
                parse_date(date_match.group(1)) if date_match else "N/A"
            )
            car_data["경매시작시간"] = "N/A"
        if deadline and " " in deadline:
            car_data["경매종료시간"] = deadline.split(" ", 1)[1][:5]
        else:
            car_data["경매종료시간"] = "N/A"
    except Exception:
        car_data.update(
            {"경매날짜": "N/A", "경매시작시간": "N/A", "경매종료시간": "N/A"}
        )
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

    return car_data


async def fetch_ids_from_page(
    context: BrowserContext, page_num: int, base_url: str, semaphore: asyncio.Semaphore
) -> list:
    """지정된 페이지에서 모든 차량 ID를 수집합니다."""
    async with semaphore:
        page = await context.new_page()
        list_page_url = f"{base_url}?i_iNowPageNo={page_num}"
        entry_cds = []
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
        except Exception:
            pass
        finally:
            await asyncio.sleep(random.uniform(1.5, 3.5))
            await page.close()
    return entry_cds


async def fetch_car_details_concurrently(
    context: BrowserContext, entry_cd: str, semaphore: asyncio.Semaphore
) -> dict | None:
    """차량 상세 정보를 병렬로 수집하기 위한 래퍼 함수입니다."""
    async with semaphore:
        page = await context.new_page()
        try:
            return await get_car_detail(page, entry_cd)
        finally:
            await asyncio.sleep(random.uniform(1.5, 2.5))
            await page.close()


def save_data_to_s3(data: list):
    """수집된 데이터를 S3에 CSV 파일로 저장합니다."""
    if not data:
        print("저장할 데이터가 없습니다.")
        return

    # 날짜 기반으로 S3 경로 생성
    now = datetime.now()
    date_ymd = now.strftime("%Y-%m-%d")
    date_ymd_plain = now.strftime("%Y%m%d")
    s3_path = S3_PATH_TEMPLATE.format(date_ymd=date_ymd, date_ymd_plain=date_ymd_plain)

    # S3 경로 파싱
    bucket, key = s3_path.replace("s3://", "").split("/", 1)

    print(f"\n수집된 {len(data)}개의 데이터를 S3 경로로 저장합니다: {s3_path}")

    # 데이터 정렬
    data.sort(key=lambda x: x.get("entry_cd", ""), reverse=True)

    # CSV 컬럼 순서 정의
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
        "경매날짜",
        "경매시작시간",
        "경매종료시간",
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
        "보관센터",
    ]

    # 데이터를 CSV 포맷의 문자열로 변환 (메모리상에서 처리)
    with io.StringIO() as csv_buffer:
        writer = csv.DictWriter(
            csv_buffer, fieldnames=fieldnames, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(data)

        # Boto3 S3 클라이언트를 사용하여 업로드
        s3_client = boto3.client("s3")
        try:
            # S3 업로드를 위해 문자열을 byte로 인코딩
            s3_client.put_object(
                Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode("utf-8-sig")
            )
            print("S3 업로드 완료!")
        except Exception as e:
            print(f"S3 업로드 중 에러 발생: {e}")


# --- 안전한 종료(Graceful Shutdown) 핸들러 ---
all_car_data_global = []
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    print("\nCtrl+C 감지! 현재까지 수집된 데이터 저장 후 종료합니다...")
    shutdown_event.set()


async def main():
    """메인 크롤링 실행 함수"""
    signal.signal(signal.SIGINT, signal_handler)

    async with async_playwright() as p:
        # EC2/Linux 환경에 최적화된 실행 옵션
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--no-first-run",
                "--no-zygote",
                "--single-process",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        )

        all_entry_cds = []
        id_tasks = []
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        try:
            # 1. ID 수집 작업 생성
            for category, base_url in BASE_URLS_TO_CRAWL.items():
                print(f"\n[{category.upper()}] 경매 목록 페이지 수 확인 중...")
                page = await context.new_page()
                try:
                    await page.goto(
                        f"{base_url}?i_iNowPageNo=1",
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    total_pages_text = await page.locator(
                        ".paging_input .totalPage"
                    ).inner_text(timeout=10000)
                    total_pages = int(total_pages_text.replace("/", "").strip())
                    print(
                        f"총 {total_pages} 페이지를 찾았습니다. 모든 페이지에서 ID를 수집합니다."
                    )
                    for page_num in range(1, total_pages + 1):
                        id_tasks.append(
                            fetch_ids_from_page(context, page_num, base_url, semaphore)
                        )
                except TimeoutError:
                    print(
                        "페이지 정보를 찾을 수 없거나 차량이 없습니다. 1 페이지만 확인합니다."
                    )
                    id_tasks.append(
                        fetch_ids_from_page(context, 1, base_url, semaphore)
                    )
                finally:
                    await page.close()

            # 2. 모든 ID 수집
            results = await tqdm.gather(*id_tasks, desc="차량 ID 수집 중")
            all_entry_cds = [item for sublist in results for item in sublist]
            unique_entry_cds = sorted(list(set(all_entry_cds)))
            print(f"\n총 {len(unique_entry_cds)}대의 고유 차량 정보를 수집합니다.")

            # 3. 모든 상세 정보 수집
            detail_tasks = [
                fetch_car_details_concurrently(context, entry_cd, semaphore)
                for entry_cd in unique_entry_cds
            ]
            global all_car_data_global
            for f in tqdm.as_completed(detail_tasks, desc="상세 정보 수집 중"):
                if shutdown_event.is_set():
                    for task in detail_tasks:
                        task.cancel()
                    break
                result = await f
                if result:
                    all_car_data_global.append(result)

        except Exception as e:
            print(f"크롤링 중 에러가 발생했습니다: {e}")
        finally:
            save_data_to_s3(all_car_data_global)
            await context.close()
            await browser.close()
            print("브라우저를 종료합니다.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        print("작업이 정상적으로 취소되었습니다.")
