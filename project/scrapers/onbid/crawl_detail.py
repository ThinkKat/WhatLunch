#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, csv
import argparse
import sys, re
import time, random
from tqdm import tqdm
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from concurrent.futures import ProcessPoolExecutor, as_completed

"""
# 종료된 경매 검색
python crawl_detail.py \
    --mode HISTORY \
    --input base/history_base_20250801_20250831.json \
    --out result/test_new.json
    --workers 10 \
    --headful

# 완료되지 않은 경매 검색
python crawl_detail.py \
    --mode NEW \
    --input base/test_new_base.json \
    --out result/test_new.json \
    --workers 10 \
    --headful
"""

SEARCH_PATH = {
    "NEW":{
        "base_url": "https://www.onbid.co.kr/op/cta/cltrdtl/collateralDetailMoveableAssetsList.do",
        "car_selector": ".info > dt:nth-child(1) > a:nth-child(1)",
        "table_selector": [
            "#Contents > div.form_wrap.mt20.mb10 > div.check_wrap.fr > table", 
            "#basicInfo #tab01_group1_basicInfo table.op_tbl_type8"
        ],
    },
    "HISTORY": {
        "base_url": "https://www.onbid.co.kr/op/bda/bidrslt/moveableResultList.do",
        "car_selector": "#Contents > table > tbody > tr > td.al > div > dl > dt > a",
        "table_selector": [
            "#Contents > div.form_wrap.mt20.mb10 > div.check_wrap.fr > table", 
            "#basicInfo #tab01_group1_basicInfo table.op_tbl_type8"
        ],
    }
}

# fn_selectDetail(...) 함수 호출을 위한 정규식
FN_CALL_RE = re.compile(r"fn_selectDetail\((.*?)\)")
def html_to_param(page, car_selector: str) -> dict:
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    first_link = soup.select_one(car_selector)
    if not first_link:
        raise RuntimeError("(html_to_param) 검색 결과에서 첫 번째 링크를 찾지 못했습니다.")

    href = first_link.get("href", "")
    m = FN_CALL_RE.search(href)
    if not m:
        raise RuntimeError(f"(html_to_param) 패턴을 찾지 못했습니다: {href}")
    
    raw_args = m.group(1)
    args = [s.strip("'") for s in raw_args.split(",")]
    # 인자 매핑 (온비드 JS에서 사용되는 순서에 맞게)
    # cltrHstrNo, cltrNo, plnmNo, pbctNo, scrnGrpCd, pbctCdtnNo, pageNo, rowNo
    return {
        "cltrHstrNo": args[0] if len(args) > 0 else None,
        "cltrNo":     args[1] if len(args) > 1 else None,
        "plnmNo":     args[2] if len(args) > 2 else None,
        "pbctNo":     args[3] if len(args) > 3 else None,
        "scrnGrpCd":  args[4] if len(args) > 4 else None,
        "pbctCdtnNo": args[5] if len(args) > 5 else None,
        "pageNo":     args[6] if len(args) > 6 else None,
        "rowNo":      args[7] if len(args) > 7 else None,
    }

def ensure_on_item_info_tab(page, mgmt_no: str, timeout_ms: int = 15000) -> None:
    page.wait_for_selector("div.tab_wrap ul", timeout=timeout_ms)
    info_tab = page.locator("a[href^='javascript:fn_goCltrImfo']").first
    if info_tab.count() == 0:
        info_tab = page.get_by_role("link", name="물건정보").first
    info_tab.click()
    
    # 테이블 존재 대기
    try:
        page.wait_for_selector("#basicInfo #tab01_group1_basicInfo table.op_tbl_type8", timeout=timeout_ms)
    except PlaywrightTimeoutError:
        raise RuntimeError(f"(ensure_on_item_info_tab) 차량 정보 테이블을 찾을 수 없습니다. (물건관리번호: {mgmt_no})")

    # 테이블에 데이터가 실제 값으로 채워질 때까지 대기 (플레이스홀더 `{...}`가 사라질 때까지)
    page.wait_for_function(
        """
        () => {
          const table = document.querySelector("#basicInfo #tab01_group1_basicInfo table.op_tbl_type8");
          if (!table) return false;
          const tds = table.querySelectorAll("tbody td");
          if (!tds.length) return false;
          // 텍스트가 비어있지 않고, {로 시작하는 플레이스홀더가 아님을 확인
          for (const td of tds) {
            const txt = td.textContent.trim();
            if (txt.length === 0) return false;                 // 아직 비어있음
            if (/^{[^}]+}$/.test(txt)) return false;             // {mnftCompNm} 같은 플레이스홀더
          }
          return true; // 모든 td가 채워짐
        }
        """,
        timeout=timeout_ms
    )
    # 네트워크가 잠잠해질 때까지 조금 더 대기
    try:
        page.wait_for_load_state("networkidle", timeout=2000)
    except:
        pass

def parse_basic_info_table(page, table_selector) -> Dict[str, str]:
    # 1) 현재 탭의 테이블
    table = page.locator(table_selector)
    rows = table.locator("tbody > tr:visible")

    data: Dict[str, str] = {}
    for i in range(rows.count()):
        tr = rows.nth(i)
        ths = tr.locator("th:visible")
        tds = tr.locator("td:visible")

        # 마지막 행(기타사항) 처리
        tr_class = tr.get_attribute("class") or ""
        if "last" in tr_class:
            if ths.count() >= 1 and tds.count() >= 1:
                key = ths.nth(0).inner_text().strip()
                val = tds.nth(0).inner_text().strip()
                if key and val:
                    # '기타사항'은 줄바꿈 유지
                    data.setdefault(key, val)
            continue

        # 일반 행: (th, td) * 2 페어
        pair_cnt = min(ths.count(), tds.count())
        for j in range(pair_cnt):
            key = ths.nth(j).inner_text().strip()
            val = tds.nth(j).inner_text().strip()
            # 2) 플레이스홀더/빈값은 스킵
            if not key or not val:
                continue
            # 3) 이미 값이 있으면 유지(첫 번째 "실값"만 채택)
            if key not in data:
                data[key] = val
    return data


def scrape_detail(args,
                  mgmt_no: str,
                  headless: bool = True,
                  timeout_ms: int = 20000) -> Dict[str, Optional[Dict]]:
    """
    단일 물건관리번호에 대한 상세 파싱.
    1) 목록 페이지 이동 → 2) 해당 mgmt_no 링크 클릭 → 3) '물건정보' 탭 → 4) 기본 정보 테이블 파싱
    """
    base_url = SEARCH_PATH[args.mode]["base_url"]
    car_selector = SEARCH_PATH[args.mode]["car_selector"]
    table_selector = SEARCH_PATH[args.mode]["table_selector"]
    with sync_playwright() as p:
        # browser = p.chromium.launch(headless=headless)
        browser = p.firefox.launch(headless=headless, timeout=timeout_ms)
        context = browser.new_context()
        page = context.new_page()

        try:
            # 1) 목록 페이지 로드
            page.goto(base_url, timeout=timeout_ms)
            time.sleep(0.5)
            page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

            # 2) 물건관리번호 입력 후 검색
            page.fill("#searchCltrMnmtNo", mgmt_no)
            page.click("#searchBtn")
            time.sleep(0.5)

            # 3) 첫 번째 결과의 링크 클릭
            page.wait_for_selector(f"a:text('{mgmt_no}')")
            params = html_to_param(page, car_selector=car_selector)
            page.evaluate(
                """(p) => {
                    return window.fn_selectDetail(
                    p.cltrHstrNo, p.cltrNo, p.plnmNo, p.pbctNo, p.scrnGrpCd, p.pbctCdtnNo, p.pageNo, p.rowNo
                    );
                }""",
                params
            )
            time.sleep(0.5)

            # 테이블 파싱
            ensure_on_item_info_tab(page, mgmt_no, timeout_ms=timeout_ms)
            basic_info = {}
            for selector in table_selector:
                basic_info = basic_info | parse_basic_info_table(page, table_selector=selector)
            return {
                "mgmt_no": mgmt_no,
                "ok": True,
                "detail_info": basic_info,
            }

        except PlaywrightTimeoutError as e:
            return {
                "mgmt_no": mgmt_no,
                "ok": False,
                "error": f"Timeout while processing {mgmt_no}: {e}"
            }
        except Exception as e:
            return {
                "mgmt_no": mgmt_no,
                "ok": False,
                "error": f"Error while processing {mgmt_no}: {e}"
            }
        finally:
            context.close()
            browser.close()

def _process_one(item, args) -> Optional[Dict]:
    mgmt = item.get("reference_num")
    if not mgmt:
        return None
    time.sleep(random.uniform(0.2, 0.8))
    base = {
        "detail_info": item
    }
    rec = scrape_detail(
        args=args,                       # argparse.Namespace 그대로 전달해도 pickle 됩니다
        mgmt_no=mgmt,
        headless=(not args.headful),
        timeout_ms=args.timeout
    )
    if not ("error" in rec):
        rec["detail_info"] = item | rec["detail_info"]
        return base | rec
    else:
        return rec

def save_results_to_csv(results, out_file):
    # ok가 true인 데이터만 추출
    valid_records = [r["detail_info"] for r in results if r.get("ok") and "detail_info" in r]

    if not valid_records:
        print("No valid records found.")
        return

    # CSV 헤더는 detail_info의 key 기준
    fieldnames = list(valid_records[0].keys())

    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in valid_records:
            writer.writerow(rec)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 JSON 파일 (배열)")
    parser.add_argument("--mode", type=str, default="NEW", help="모드 (NEW 또는 HISTORY)", choices=["NEW", "HISTORY"])
    parser.add_argument("--headful", action="store_true", help="브라우저 창 표시")
    parser.add_argument("--timeout", type=int, default=200000, help="각 단계 타임아웃(ms)")
    parser.add_argument("--workers", type=int, default=1, help="동시 프로세스 수(과하면 차단될 수 있음)")
    parser.add_argument("--out", type=str, default="result.csv")
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        arr = json.load(f)

    results: List[Dict] = []
    # 멀티프로세싱 실행
    ok_cnt = 0
    err_cnt = 0
    timeout_cnt = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(_process_one, it, args) for it in arr]
        with tqdm(total=len(futures), desc="Processing", dynamic_ncols=True) as pbar:
            pbar.set_postfix({"ok": ok_cnt, "err": err_cnt, "timeout": timeout_cnt})
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if not res:
                        # reference_num 없어서 None 반환한 경우 등
                        err_cnt += 1
                    else:
                        results.append(res)
                        if res.get("ok", False):
                            ok_cnt += 1
                        else:
                            err_cnt += 1
                            # 메시지로 타임아웃 추정 집계(선택)
                            if "Timeout" in str(res.get("error", "")):
                                timeout_cnt += 1
                except Exception as e:
                    # 워커 프로세스 자체 예외
                    err_cnt += 1
                    print(f'error at {res.get("mgmt_no", "")}')
                    results.append({"ok": False, "error": f"Worker exception: {e}"})
                finally:
                    pbar.update(1)
                    pbar.set_postfix({"ok": ok_cnt, "err": err_cnt, "timeout": timeout_cnt})

    save_results_to_csv(results, args.out)
    print(f"[OK] saved {len(results)} record(s) to {args.out}")

if __name__ == "__main__":
    sys.exit(main())