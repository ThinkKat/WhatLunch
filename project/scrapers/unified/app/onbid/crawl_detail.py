#!/usr/bin/env python3
import argparse
import csv
import logging
import random
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

SEARCH_PATH = {
    "HISTORY": {
        "base_url": "https://www.onbid.co.kr/op/cta/cltrdtl/cntrDtlsList.do",
        "car_selector": "#searchCltrMnmtNo",
        "table_selector": ["#basicInfoTable", "#addInfoTable"],
    }
}


def parse_basic_info_table(page, table_selector):
    data = {}
    try:
        rows = page.query_selector_all(f"{table_selector} tr")
        for r in rows:
            cols = r.query_selector_all("td")
            if len(cols) >= 2:
                key = cols[0].inner_text().strip()
                val = cols[1].inner_text().strip()
                data[key] = val
    except Exception:
        pass
    return data


def ensure_on_item_info_tab(page, mgmt_no, timeout_ms=20000):
    try:
        page.wait_for_selector("#basicInfoTable", timeout=timeout_ms)
    except PlaywrightTimeoutError:
        raise RuntimeError(f"{mgmt_no}: Item info tab not loaded")


def scrape_detail(
    args, mgmt_no: str, headless: bool = True, timeout_ms: int = 20000, retries: int = 3
):
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

                    # 검색 폼 입력/검색
                    page.fill(car_selector, mgmt_no)
                    page.click("#searchBtn")

                    # ✅ 올바른 선택자
                    locator = page.locator(f"a:has-text('{mgmt_no}')").first
                    locator.wait_for(state="visible", timeout=timeout_ms)

                    # 상세 페이지 이동
                    with page.expect_navigation(timeout=timeout_ms):
                        locator.click()

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
            logging.warning(f"[Attempt {attempt + 1}/{retries}] {mgmt_no} failed: {e}")
            if attempt < retries - 1:
                time.sleep(random.uniform(5, 10))

    return {
        "mgmt_no": mgmt_no,
        "ok": False,
        "error": f"Failed after {retries} attempts: {last_exception}",
    }


def _process_one(item, args):
    mgmt_no = str(item.get("mgmtNo", "")).strip()
    if not mgmt_no:
        return None
    return scrape_detail(
        args,
        mgmt_no,
        headless=(not args.headful),
        timeout_ms=args.timeout,
        retries=args.retries,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Base JSON file path")
    parser.add_argument("--out", required=True, help="Output CSV path")
    parser.add_argument("--mode", default="HISTORY", choices=["HISTORY"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30000)
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    input_path = Path(args.input)
    if not input_path.exists():
        logging.error(f"Input file not found: {input_path}")
        return

    arr = json.loads(input_path.read_text(encoding="utf-8"))

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(_process_one, it, args) for it in arr]
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mgmt_no", "ok", "detail_info", "error"])
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    logging.info(f"Done. Wrote {len(results)} rows to {out_path}")


if __name__ == "__main__":
    main()
