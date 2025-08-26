#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, csv
import argparse
import sys, re
import time, random
import logging
from tqdm import tqdm
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from concurrent.futures import ProcessPoolExecutor, as_completed

# ... (SEARCH_PATH, FN_CALL_RE 등 이전과 동일한 부분은 생략) ...
SEARCH_PATH = {
    "NEW": {
        "base_url": "https://www.onbid.co.kr/op/cta/cltrdtl/collateralDetailMoveableAssetsList.do",
        "car_selector": ".info > dt:nth-child(1) > a:nth-child(1)",
        "table_selector": [
            "#Contents > div.form_wrap.mt20.mb10 > div.check_wrap.fr > table",
            "#basicInfo #tab01_group1_basicInfo table.op_tbl_type8",
        ],
    },
    "HISTORY": {
        "base_url": "https://www.onbid.co.kr/op/bda/bidrslt/moveableResultList.do",
        "car_selector": "#Contents > table > tbody > tr > td.al > div > dl > dt > a",
        "table_selector": [
            "#Contents > div.form_wrap.mt20.mb10 > div.check_wrap.fr > table",
            "#basicInfo #tab01_group1_basicInfo table.op_tbl_type8",
        ],
    },
}

FN_CALL_RE = re.compile(r"fn_selectDetail\((.*?)\)")


def html_to_param(page, car_selector: str) -> dict:
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    first_link = soup.select_one(car_selector)
    if not first_link:
        raise RuntimeError(
            "(html_to_param) 검색 결과에서 첫 번째 링크를 찾지 못했습니다."
        )

    href = first_link.get("href", "")
    m = FN_CALL_RE.search(href)
    if not m:
        raise RuntimeError(f"(html_to_param) 패턴을 찾지 못했습니다: {href}")

    raw_args = m.group(1)
    args = [s.strip("'") for s in raw_args.split(",")]
    return {
        "cltrHstrNo": args[0] if len(args) > 0 else None,
        "cltrNo": args[1] if len(args) > 1 else None,
        "plnmNo": args[2] if len(args) > 2 else None,
        "pbctNo": args[3] if len(args) > 3 else None,
        "scrnGrpCd": args[4] if len(args) > 4 else None,
        "pbctCdtnNo": args[5] if len(args) > 5 else None,
        "pageNo": args[6] if len(args) > 6 else None,
        "rowNo": args[7] if len(args) > 7 else None,
    }


def ensure_on_item_info_tab(page, mgmt_no: str, timeout_ms: int = 15000) -> None:
    page.wait_for_selector("div.tab_wrap ul", timeout=timeout_ms)
    info_tab = page.locator("a[href^='javascript:fn_goCltrImfo']").first
    if info_tab.count() == 0:
        info_tab = page.get_by_role("link", name="물건정보").first
    info_tab.click()

    try:
        page.wait_for_selector(
            "#basicInfo #tab01_group1_basicInfo table.op_tbl_type8", timeout=timeout_ms
        )
    except PlaywrightTimeoutError:
        raise RuntimeError(
            f"(ensure_on_item_info_tab) 차량 정보 테이블을 찾을 수 없습니다. (물건관리번호: {mgmt_no})"
        )

    page.wait_for_function(
        """
        () => {
          const table = document.querySelector("#basicInfo #tab01_group1_basicInfo table.op_tbl_type8");
          if (!table) return false;
          const tds = table.querySelectorAll("tbody td");
          if (!tds.length) return false;
          for (const td of tds) {
            const txt = td.textContent.trim();
            if (txt.length === 0) return false;
            if (/^{[^}]+}$/.test(txt)) return false;
          }
          return true;
        }
        """,
        timeout=timeout_ms,
    )
    try:
        page.wait_for_load_state("networkidle", timeout=2000)
    except:
        pass


def parse_basic_info_table(page, table_selector) -> Dict[str, str]:
    table = page.locator(table_selector)
    rows = table.locator("tbody > tr:visible")
    data: Dict[str, str] = {}
    for i in range(rows.count()):
        tr = rows.nth(i)
        ths = tr.locator("th:visible")
        tds = tr.locator("td:visible")
        tr_class = tr.get_attribute("class") or ""
        if "last" in tr_class:
            if ths.count() >= 1 and tds.count() >= 1:
                key = ths.nth(0).inner_text().strip()
                val = tds.nth(0).inner_text().strip()
                if key and val:
                    data.setdefault(key, val)
            continue
        pair_cnt = min(ths.count(), tds.count())
        for j in range(pair_cnt):
            key = ths.nth(j).inner_text().strip()
            val = tds.nth(j).inner_text().strip()
            if not key or not val:
                continue
            if key not in data:
                data[key] = val
    return data


def scrape_detail(
    args, mgmt_no: str, headless: bool = True, timeout_ms: int = 20000, retries: int = 3
) -> Dict[str, Optional[Dict]]:
    """
    단일 물건관리번호에 대한 상세 파싱. 오류 발생 시 재시도 로직 추가.
    """
    base_url = SEARCH_PATH[args.mode]["base_url"]
    car_selector = SEARCH_PATH[args.mode]["car_selector"]
    table_selector = SEARCH_PATH[args.mode]["table_selector"]

    last_exception = None
    for attempt in range(retries):
        try:
            with sync_playwright() as p:
                browser = p.firefox.launch(headless=headless, timeout=timeout_ms)
                context = browser.new_context()
                page = context.new_page()

                try:
                    page.goto(base_url, timeout=timeout_ms)
                    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                    page.fill("#searchCltrMnmtNo", mgmt_no)
                    page.click("#searchBtn")
                    page.wait_for_selector(f"a:text('{mgmt_no}')")
                    params = html_to_param(page, car_selector=car_selector)
                    page.evaluate(
                        """(p) => {
                            return window.fn_selectDetail(
                            p.cltrHstrNo, p.cltrNo, p.plnmNo, p.pbctNo, p.scrnGrpCd, p.pbctCdtnNo, p.pageNo, p.rowNo
                            );
                        }""",
                        params,
                    )
                    ensure_on_item_info_tab(page, mgmt_no, timeout_ms=timeout_ms)
                    basic_info = {}
                    for selector in table_selector:
                        basic_info.update(
                            parse_basic_info_table(page, table_selector=selector)
                        )

                    return {"mgmt_no": mgmt_no, "ok": True, "detail_info": basic_info}

                finally:
                    context.close()
                    browser.close()

        except Exception as e:
            last_exception = e
            logging.warning(
                f"[Attempt {attempt + 1}/{retries}] Failed to process {mgmt_no}: {e}"
            )
            if attempt < retries - 1:
                time.sleep(random.uniform(5, 10))  # 재시도 전 대기

    return {
        "mgmt_no": mgmt_no,
        "ok": False,
        "error": f"Failed after {retries} attempts: {last_exception}",
    }


def _process_one(item, args) -> Optional[Dict]:
    mgmt = item.get("reference_num")
    if not mgmt:
        return None
    time.sleep(random.uniform(0.2, 0.8))
    rec = scrape_detail(
        args=args,
        mgmt_no=mgmt,
        headless=(not args.headful),
        timeout_ms=args.timeout,
        retries=args.retries,
    )
    if rec.get("ok"):
        rec["detail_info"] = item | rec["detail_info"]
    return rec


def clean_and_process_records(records: List[Dict]) -> List[Dict]:
    processed_list = []
    for rec in records:
        if rec.get("open_datetime"):
            rec["open_datetime"] = rec["open_datetime"].split(" ")[0]
        if rec.get("제조사 / 모델명"):
            parts = rec["제조사 / 모델명"].split(" / ", 1)
            rec["브랜드"] = parts[0].strip()
            rec["모델명"] = parts[1].strip() if len(parts) > 1 else ""
        if rec.get("연료"):
            rec["연료"] = rec["연료"].split("(")[0].strip()
        if rec.get("배기량"):
            rec["배기량"] = re.sub(r"[^0-9]", "", str(rec["배기량"]))
        if rec.get("주행거리"):
            rec["주행거리"] = re.sub(r"[^0-9]", "", str(rec["주행거리"]))
        processed_list.append(rec)
    return processed_list


def save_results_to_csv(results, out_file):
    valid_records = [
        r["detail_info"] for r in results if r.get("ok") and "detail_info" in r
    ]

    # 데이터가 없으면 파일을 생성하지 않고 함수 종료
    if not valid_records:
        logging.info("No valid records found. CSV file will not be created.")
        return

    # 기본 헤더 정의
    preferred_order = [
        "reference_num",
        "open_datetime",
        "브랜드",
        "모델명",
        "연식",
        "연료",
        "배기량",
        "주행거리",
        "변속기",
        "차량번호",
        "minimum_bid_price",
        "estimated_price",
        "bid_price",
        "bid_result",
        "bid_price_rate",
    ]
    unwanted_cols = {"기타사항", "제조사 / 모델명", "제조사", "차종"}

    cleaned_records = clean_and_process_records(valid_records)
    all_keys = set(key for rec in cleaned_records for key in rec.keys())
    final_headers = preferred_order + sorted(
        [
            key
            for key in all_keys
            if key not in preferred_order and key not in unwanted_cols
        ]
    )

    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=final_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cleaned_records)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 JSON 파일 (배열)")
    parser.add_argument(
        "--mode",
        type=str,
        default="NEW",
        help="모드 (NEW 또는 HISTORY)",
        choices=["NEW", "HISTORY"],
    )
    parser.add_argument("--headful", action="store_true", help="브라우저 창 표시")
    parser.add_argument(
        "--timeout", type=int, default=30000, help="각 단계 타임아웃(ms)"
    )
    parser.add_argument("--workers", type=int, default=1, help="동시 프로세스 수")
    parser.add_argument("--retries", type=int, default=3, help="실패 시 재시도 횟수")
    parser.add_argument("--out", type=str, default="result.csv")
    parser.add_argument(
        "--log-file", type=str, default="crawler.log", help="로그 파일 경로"
    )
    args = parser.parse_args()

    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(args.log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info("Crawler started with args: %s", args)

    try:
        with open(args.input, "r", encoding="utf-8") as f:
            arr = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to read or parse input file {args.input}: {e}")
        arr = []  # 입력 파일이 없거나 비어있으면 빈 리스트로 처리

    results: List[Dict] = []
    ok_cnt, err_cnt = 0, 0

    if arr:  # 입력 데이터가 있을 때만 멀티프로세싱 실행
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(_process_one, it, args) for it in arr]
            with tqdm(
                total=len(futures), desc="Processing", dynamic_ncols=True
            ) as pbar:
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                        if not res:
                            err_cnt += 1
                        else:
                            results.append(res)
                            if res.get("ok", False):
                                ok_cnt += 1
                            else:
                                err_cnt += 1
                                logging.error(
                                    f"Failed to process {res.get('mgmt_no')}: {res.get('error')}"
                                )
                    except Exception as e:
                        err_cnt += 1
                        logging.critical(f"A worker process failed unexpectedly: {e}")
                    finally:
                        pbar.update(1)
                        pbar.set_postfix({"ok": ok_cnt, "err": err_cnt})

    save_results_to_csv(results, args.out)
    logging.info(f"Crawling finished. OK: {ok_cnt}, ERR: {err_cnt}")
    logging.info(f"Saved processed records to {args.out}")


if __name__ == "__main__":
    sys.exit(main())
