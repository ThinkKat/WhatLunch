#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import random
import re
import sys
import time
from tqdm import tqdm
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

"""
# 종료된 경매 검색
python crawl_base.py \
    --mode HISTORY \
    --from 2024-08-02 --to 2025-08-01 \
    --categories 12101 12102 12103 \
    --out base/history_base.json \
    --max-pages 500

# 완료되지 않은 경매 검색
python crawl_base.py \
    --mode NEW \
    --from 2025-08-01 --to 2025-08-28 \
    --categories 12101 12102 12103 \
    --out base/new_base.json \
    --max-pages 50
"""


# 카테고리 코드 매핑
CTGR_LARGE = "12000"   # 대분류: 자동차/운송장비
CTGR_MID   = "12100"   # 중분류: 자동차
CTGR_SMALL_NAMES = {
    "12101": "승용차",
    "12102": "SUV",
    "12103": "승합차",
}

# additional col 형식
# lamba 입력 = 1개의 row
# labmda 출력 = row를 col로 가공
SEARCH_PATH = {
    "NEW": {
        "base_url": "https://www.onbid.co.kr/op/cta/cltrdtl/newCollateralDetailMoveableAssetsList.do",
        "col_name": [
            "item_info_raw", "datetime", "price", "status",
        ],
        "additional_col": {
            "reference_num": lambda x: x[0].get_text(" ", strip=True).split()[1],
            "open_datetime": lambda x: " ".join(x[1].get_text(" ", strip=True).split()[-5:-3]),
            "close_datetime": lambda x: " ".join(x[1].get_text(" ", strip=True).split()[-2:]), 
            "minimum_bid_price": lambda x: x[2].get_text(" ", strip=True).split()[0],
            "estimated_price": lambda x: x[2].get_text(" ", strip=True).split()[1],
        }
    },
    "HISTORY": {
        "base_url": "https://www.onbid.co.kr/op/bda/bidrslt/moveableResultList.do",
        "col_name": [
            "item_info_raw", "minimum_bid_price", "bid_price", "bid_price_rate", "bid_result", "open_datetime", 
        ],
        "additional_col": {
            "reference_num": lambda x: x[0].get_text(" ", strip=True).split()[0], 
        }
    },
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ko,en;q=0.9",
    "Referer": "https://www.onbid.co.kr/",
}

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def parse_int(text: str) -> Optional[int]:
    if text is None:
        return None
    m = re.search(r"([\d,]+)", text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None
    
def extract_item_mgmt_no(text: str) -> Optional[str]:
    m = re.search(r"물건관리번호\s*[:\-]?\s*([0-9\-]+)", text)
    return m.group(1) if m else None

def split_vehicle_info(item_cell_text: str) -> Tuple[Optional[str], Optional[str]]:
    text = norm_space(item_cell_text)
    mgmt = extract_item_mgmt_no(text)
    veh = text
    if mgmt:
        # '물건관리번호 xxx' 구간 제거 시도
        veh = re.sub(r"물건관리번호\s*[:\-]?\s*" + re.escape(mgmt), "", veh).strip()
    return mgmt, veh or None

def parse_table_rows(mode, html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.op_tbl_type6")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []
    
    rows = []
    for tr in tbody.find_all("tr", recursive=False):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 6:
            # 빈 행/구분 행 등
            continue

        row = {col: None for col in SEARCH_PATH[mode]["col_name"]}
        # base column
        for i, col in enumerate(SEARCH_PATH[mode]["col_name"]):
            item = tds[i]
            if "price" in col:
                item = parse_int(item.get_text(" ", strip=True))
            else:
                item = norm_space(item.get_text(" ", strip=True))

            row[col] = item

        # additional column
        for i, (col, lda) in enumerate(SEARCH_PATH[mode]["additional_col"].items()):
            item = lda(tds)
            if "price" in col:
                item = parse_int(item)
            else:
                item = norm_space(item)
            row[col] = item
        
        rows.append(row)
    return rows


def build_form_payload(
    *,
    date_from: str,
    date_to: str,
    ctgr_small: str,
    page_index: int,
) -> Dict[str, str]:
    payload = {
        # 날짜
        "searchBidDateFrom": date_from,
        "searchBidDateTo": date_to,
        # 용도 분류 (대/중/소)
        "searchCtgrId1": CTGR_LARGE,  # 12000
        "searchCtgrId2": CTGR_MID,    # 12100
        "searchCtgrId": ctgr_small,   # 12101/12102/12103
        # 페이징
        "pageIndex": str(page_index),
        # 주소 검색 합본값(빈 값 허용)
        "searchAddr": "",
        # 일부 hidden 필드 (없어도 서버가 수용하나 호환성 위해 포함)
        "viewGbn": "",
        "searchSiDo": "",
        "searchSiGunGu": "",
        "searchEmd": "",
        # 기타: 폼 유효성 처리 우회를 위해 빈 값 유지
        "searchCltrMnmtNo": "",
    }
    return payload


def fetch_page(url, session: requests.Session, payload: Dict[str, str]) -> str:
    r = session.post(url, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def scrape_range(
    *,
    args,
    date_from: str,
    date_to: str,
    categories: Iterable[str],
    max_pages: int,
    pause: Tuple[float, float] = (0.6, 1.4),
):
    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows = []
    
    for small in categories:
        print(f"[info] 카테고리 {small} ({CTGR_SMALL_NAMES.get(small, '알 수 없음')}) 수집 시작")
        for page in tqdm(range(1, max_pages + 1)):
            payload = build_form_payload(
                date_from=date_from, date_to=date_to, ctgr_small=small, page_index=page
            )
            html = fetch_page(SEARCH_PATH[args.mode]["base_url"], session, payload)
            rows = parse_table_rows(args.mode, html)
            if not rows:
                break
            all_rows.extend(rows)
            # 예의상 랜덤 대기
            time.sleep(random.uniform(*pause))
    return all_rows

def save_json(rows, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def valid_date(s: str) -> str:
    # YYYY-MM-DD 검증
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except Exception:
        raise argparse.ArgumentTypeError(f"날짜 형식이 올바르지 않습니다(YYYY-MM-DD): {s}")


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="ONBID 동산 입찰결과 스크래퍼")
    default_to = datetime.today()
    default_from = default_to - timedelta(days=30)

    p.add_argument("--mode", choices=["NEW", "HISTORY"], default="NEW",
                   help="수집 모드 (NEW: 신규, HISTORY: 과거)")
    p.add_argument("--from", dest="date_from", type=valid_date,
                   default=default_from.strftime("%Y-%m-%d"), help="개찰일자 조회 시작 (YYYY-MM-DD)")
    p.add_argument("--to", dest="date_to", type=valid_date,
                   default=default_to.strftime("%Y-%m-%d"), help="개찰일자 조회 종료 (YYYY-MM-DD)")
    p.add_argument("--categories", nargs="+", default=["12101", "12102", "12103"],
                   help="소분류 코드 리스트 (예: 12101 12102 12103)")
    p.add_argument("--max-pages", type=int, default=30, help="카테고리별 최대 페이지 수집 한도")
    p.add_argument("--out", dest="json_out", default=None, help="JSON 저장 경로(선택)")

    args = p.parse_args(argv)

    # 1년 범위 체크 (서버 제약 회피용 Best-effort)
    d_from = datetime.strptime(args.date_from, "%Y-%m-%d")
    d_to = datetime.strptime(args.date_to, "%Y-%m-%d")
    if (d_to - d_from).days > 365:
        p.error("개찰일자 검색 범위는 최대 1년입니다. --from/--to를 조정하세요.")


    pause_tuple = (0.6, 1.4)
    rows = scrape_range(
        args = args,
        date_from=args.date_from,
        date_to=args.date_to,
        categories=args.categories,
        max_pages=args.max_pages,
        pause=pause_tuple,
    )

    if not rows:
        print("[info] 수집된 행이 없습니다.")
    else:
        print(f"[info] 수집 완료: {len(rows)}건")

    if args.json_out:
        save_json(rows, args.json_out)
        print(f"JSON 저장 완료: {args.json_out}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
