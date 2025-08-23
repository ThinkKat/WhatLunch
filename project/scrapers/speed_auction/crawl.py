#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import json
import time
import random
import argparse
import requests
import threading
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin, urlparse, parse_qsl
from concurrent.futures import ThreadPoolExecutor, as_completed
"""
python crawl.py \
    --start-date  2024-08-19 \
    --end-date    2025-08-20 \
    --mode        end
"""

# =========================
# 고정 설정 (필요시 코드에서 수정)
# =========================
BASE_URL = "http://www.xn--289ar2ubyal2b.kr"
LIST_PATH = "/v1/s01/01.htm"                    # 목록 화면
DETAIL_POST_PATH = "/auctionInfo/view.php"      # 상세는 POST로 view.php로 전달
START_PAGE = 1                                  # 목록 시작 페이지
END_PAGE = 99                                   # 목록 종료 페이지(고정)
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": urljoin(BASE_URL, LIST_PATH),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


# 경매결과 매핑
RESULT_MAP = {
    "ongoing": "1,2,8,17",          # 진행중
    "end": "3,4,5,6,7,15,16",       # 종결
    "total": "99",                  # 전체
}

# 최종 결과에서 제외할 키
EXCLUDE_KEYS = {
    "court", "auction_datetime", "auction_weekday",
    "dept_name", "dept_phone", "storage_location", "case_received_date",
    "tenant_info", "transmission2"
}

THREAD_LOCAL = threading.local()

# =========================
# 인코딩 / 정규화 유틸
# =========================
def _decode_best(raw: bytes) -> str:
    for enc in ("euc-kr", "cp949", "utf-8"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("euc-kr", errors="replace")

WS_RE = re.compile(r"\s+")
KOREAN_RE = re.compile(r"[가-힣]")

def _norm(s: str) -> str:
    return WS_RE.sub(" ", (s or "").strip())

def _norm_key(k: str) -> str:
    k = _norm(k).rstrip(":：")
    if KOREAN_RE.search(k):
        k = k.replace(" ", "")  # '최 저 가' → '최저가'
    return k

def _int_or_none(x: str):
    if not x: return None
    x = re.sub(r"[^\d\-]", "", x)
    try:
        return int(x)
    except Exception:
        return None

def _money(x: str):
    if not x: return None
    m = re.search(r"(\d[\d,]*)\s*원", x) or re.search(r"\d[\d,]*", x)
    return int(m.group(0).replace(",", "").replace("원", "")) if m else None

def _cc(x: str):
    if not x: return None
    m = re.search(r"(\d[\d,]*)\s*cc", x, re.I)
    return int(m.group(1).replace(",", "")) if m else None

def _km(x: str):
    if not x: return None
    m = re.search(r"(\d[\d,]*)\s*km", x, re.I)
    return int(m.group(1).replace(",", "")) if m else None

def _first(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "":
            return v
    return None

# =========================
# 테이블 파싱(중첩/노이즈 대응)
# =========================
def _is_tr_of_table(tr: Tag, table: Tag) -> bool:
    return tr.find_parent("table") is table

def _cell_text_no_nested(cell: Tag) -> str:
    removed = []
    for t in cell.find_all("table"):
        removed.append(t.extract())
    txt = _norm(cell.get_text(" ", strip=True))
    for t in removed:
        cell.append(t)  # 복원 필요 없으면 생략 가능
    return txt

def _rows(table: Tag):
    rows = []
    for tr in table.find_all("tr"):
        if not _is_tr_of_table(tr, table):
            continue
        cells = tr.find_all(["th","td"], recursive=False) or tr.find_all(["th","td"])
        vals = [_cell_text_no_nested(c) for c in cells]
        if any(_norm(v) for v in vals):
            rows.append(vals)
    return rows

def _header_like(row):  # 표형 헤더 감지
    return len(row) >= 2 and all(_norm(c) for c in row)

def _is_matrix(rows):
    return len(rows) >= 2 and _header_like(rows[0])

def _matrix_to_dicts(rows):
    hdr = [_norm_key(h) for h in rows[0]]
    out = []
    for r in rows[1:]:
        d = {}
        for i, h in enumerate(hdr):
            if not h: continue
            d[h] = _norm(r[i]) if i < len(r) else ""
        if any(d.values()):
            out.append(d)
    return out

def _kv_from_rows(rows):
    kv = {}
    for row in rows:
        i, L = 0, len(row)
        while i < L:
            key = _norm(row[i])
            if not key:
                i += 1; continue
            # '키:' 패턴 (뒤 꼬리까지 한 값으로)
            if key.endswith(":") and i + 1 < L:
                val = _norm(row[i+1])
                extras = [_norm(x) for x in row[i+2:] if _norm(x)]
                if extras:
                    val = _norm(val + " " + " ".join(extras))
                kv[_norm_key(key)] = val
                break
            # 일반 키-값 쌍
            if i + 1 < L:
                kv[_norm_key(key)] = _norm(row[i+1])
                i += 2
            else:
                i += 1
    return kv

# =========================
# 상단 요약 텍스트(법원/사건/담당/전화)
# =========================
def _case_info(texts):
    j = " ".join(texts)
    info = {}
    m = re.search(r"([가-힣]+지방법원)", j)
    if m: info["court"] = m.group(1)
    m = re.search(r"(\d{4}타경\d+)", j)
    if m: info["case_number"] = m.group(1)
    m = re.search(r"담당계\s*[:：]?\s*([^\s()]+)", j)
    if m: info["dept_name"] = m.group(1)
    m = re.search(r"\(?0\d[\d\-() ]+\)?", j)
    if m: info["dept_phone"] = m.group(0).strip("() ")
    return info

# =========================
# 스키마/별칭
# =========================
SCHEMA = {
    # 상단 요약(출력 제외 대상이지만 내부 파싱은 해둠)
    "court": None, "case_number": None, "auction_datetime": None,
    "auction_weekday": None, "dept_name": None, "dept_phone": None,
    "storage_location": None, "case_received_date": None, "tenant_info": None,

    # 핵심 스펙(출력 유지)
    "item_type": None, "claimant": None, "appraisal_price": None, "min_price": None,
    "vehicle_name": None, "debtor": None, "deposit": None, "year": None, "owner": None,
    "displacement_cc": None, "transmission": None, "claim_amount": None,
    "bid_method": None, "dividend_deadline": None, "commencement_date": None,
    "manufacturer": None, "color": None, "mileage_km": None, "plate_number": None,
    "fuel": None, "transmission2": None, "registration_date": None, "domicile": None,
    "vin": None, "inspection_period": None, "notes": None,
    "bid_history": [],
}

ALIASES = {
    "물건종별": ["물건종별"],
    "채권자": ["채권자","채 권 자"],
    "감정가": ["감정가","감 정 가"],
    "최저가": ["최저가","최 저 가","최저매각금액"],
    "차명": ["차명"],
    "채무자": ["채무자"],
    "보증금": ["보증금","보 증 금"],
    "년식": ["년식"],
    "소유자": ["소유자"],
    "배기량": ["배기량","배 기 량"],
    "변속기": ["변속기"],
    "청구금액": ["청구금액"],
    "입찰방법": ["입찰방법"],
    "배당종기일": ["배당종기일"],
    "개시결정": ["개시결정"],
    "제조사": ["제조사","제 조 사"],
    "색상": ["색상","색 상"],
    "주행거리": ["주행거리"],
    "등록번호": ["등록번호"],
    "사용연료": ["사용연료"],
    "등록일자": ["등록일자"],
    "사용본거지": ["사용본거지","보관장소"],
    "차대번호": ["차대번호"],
    "검사기간": ["검사기간"],
    "기타": ["기타","기 타"],
    # 상단/보조
    "매각기일": ["매각기일"],
    "보관장소": ["보관장소"],
    "사건접수": ["사건접수"],
}

# =========================
# 상세 파서 (스키마 적용 + 제외 키 제거)
# =========================
def parse_detail_to_schema(resp: requests.Response) -> dict:
    soup = BeautifulSoup(_decode_best(resp.content), "html.parser")

    # 모든 테이블 → 행 추출
    tables = soup.find_all("table")
    table_rows = []
    for t in tables:
        rows = _rows(t)
        if rows:
            table_rows.append((t, rows))

    # 전체 텍스트(상단 요약/요일 보완용)
    texts = [c for _, rows in table_rows for r in rows for c in r if c]

    # 초기 스키마
    data = {k: ([] if isinstance(v, list) else None) for k, v in SCHEMA.items()}

    # 상단 요약(내부 보유만; 일부는 exclude)
    info = _case_info(texts)
    data.update({k: v for k, v in info.items() if k in data})
    data["winning_bid"] = None  # 낙찰가(별도 처리)

    # 표형/kv 분리
    kv = {}
    for _, rows in table_rows:
        if _is_matrix(rows):
            hdr = [_norm_key(h) for h in rows[0]]
            # 기일현황만 리스트로 보존
            if "회차" in hdr:
                items = _matrix_to_dicts(rows)
                # 금액 숫자화
                for it in items:
                    if "최저매각금액" in it:
                        it["최저매각금액_원"] = _money(it["최저매각금액"])
                    if ("회차" in it) and ("낙찰" in it["회차"]):
                        data["winning_bid"] = _money(it["회차"])
                data["bid_history"] = items
                
        else:
            kv.update(_kv_from_rows(rows))

    # 상단 '매각기일 : ... (수)' 합치기
    if "매각기일" in kv and data.get("auction_datetime") is None:
        data["auction_datetime"] = _norm(kv["매각기일"])
        # 요일 별도 추출
        m = re.search(r"\(([가-힣])\)", kv["매각기일"])
        if m: data["auction_weekday"] = m.group(1)

    # 사건접수(텍스트 덩어리에서 보완)
    if data.get("case_received_date") is None:
        for tx in texts:
            if "사건접수" in tx:
                m = re.search(r"사건접수\s*(\d{4}-\d{2}-\d{2})", tx)
                if m:
                    data["case_received_date"] = m.group(1)
                    break

    # 스키마 주입
    def any_of(names):
        for name in names:
            if name in kv and kv[name]:
                return kv[name]
        return None

    data["item_type"]       = any_of(ALIASES["물건종별"])
    data["claimant"]        = any_of(ALIASES["채권자"])
    data["appraisal_price"] = _money(any_of(ALIASES["감정가"]) or "")
    data["min_price"]       = _money(any_of(ALIASES["최저가"]) or "")
    data["vehicle_name"]    = any_of(ALIASES["차명"])
    data["debtor"]          = any_of(ALIASES["채무자"])
    data["deposit"]         = _money(any_of(ALIASES["보증금"]) or "")
    data["year"]            = _int_or_none(any_of(ALIASES["년식"]) or "")
    data["owner"]           = any_of(ALIASES["소유자"])
    data["displacement_cc"] = _cc(any_of(ALIASES["배기량"]) or "")
    data["transmission"]    = any_of(ALIASES["변속기"])
    data["claim_amount"]    = _money(any_of(ALIASES["청구금액"]) or "")
    data["bid_method"]      = any_of(ALIASES["입찰방법"])
    data["dividend_deadline"] = any_of(ALIASES["배당종기일"])
    data["commencement_date"] = any_of(ALIASES["개시결정"])

    data["manufacturer"]      = any_of(ALIASES["제조사"])
    data["color"]             = any_of(ALIASES["색상"])
    data["mileage_km"]        = _km(any_of(ALIASES["주행거리"]) or "")
    data["plate_number"]      = any_of(ALIASES["등록번호"])
    data["fuel"]              = any_of(ALIASES["사용연료"])
    data["registration_date"] = any_of(ALIASES["등록일자"])
    data["domicile"]          = _first(any_of(ALIASES["사용본거지"]), data.get("storage_location"))
    data["vin"]               = any_of(ALIASES["차대번호"])
    data["inspection_period"] = any_of(ALIASES["검사기간"])
    data["notes"]             = any_of(ALIASES["기타"])

    # 출력에서 제외할 키 제거
    cleaned = {k: v for k, v in data.items() if k not in EXCLUDE_KEYS}
    return cleaned

# =========================
# 목록에서 상세 URL 수집
# =========================
NEW_WINDOW_RE = re.compile(r"""NewWindow\(\s*['"]([^'"]+)['"]""", re.I)

def fetch_list_urls(session: requests.Session, start_page: int, end_page: int, base_payload: dict):
    urls = []
    # 탭 진입(세션/쿠키 확보)
    session.get(urljoin(BASE_URL, LIST_PATH + "?mtype=4"), headers=HEADERS, timeout=15)

    for page in range(start_page, end_page + 1):
        payload = dict(base_payload)
        payload["page"] = str(page)
        resp = session.post(urljoin(BASE_URL, LIST_PATH), data=payload, headers=HEADERS, timeout=15)
        resp.encoding = "euc-kr"
        html = resp.text

        found = NEW_WINDOW_RE.findall(html)
        if not found:
            print(f"[INFO] total_urls # : {len(urls)} ({page} pages)")
            break

        # 절대경로화
        abs_urls = [urljoin(BASE_URL, u) for u in found]
        urls.extend(abs_urls)
        msg = f"[INFO] page {page}: {len(abs_urls)} urls, total urls {len(urls)}"
        print(msg.ljust(60), end="\r")  # ← 같은 줄 갱신
        time.sleep(random.uniform(0.5, 0.9))
    return urls

# =========================
# switch.php 쿼리 → view.php POST payload
# =========================
def switch_url_to_view_payload(switch_url: str) -> dict:
    q = dict(parse_qsl(urlparse(switch_url).query, keep_blank_values=True))
    payload = {
        "courtNo": q.get("courtNo", ""),
        "courtNo2": q.get("courtNo2", ""),
        "eventNo1": q.get("eventNo1", ""),
        "eventNo2": q.get("eventNo2", ""),
        "objNo": q.get("objNo", ""),
    }
    return payload

# =========================
# CLI 인자 처리
# =========================
def split_ymd(s: str):
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", s.strip())
    if not m:
        raise ValueError(f"날짜 형식이 잘못되었습니다: {s} (YYYY-MM-DD)")
    return m.group(1), m.group(2), m.group(3)

def get_session():
    # 스레드마다 독립 Session
    if not hasattr(THREAD_LOCAL, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        THREAD_LOCAL.session = s
    return THREAD_LOCAL.session

def fetch_and_parse(switch_url):
    # 1) payload 생성
    view_payload = switch_url_to_view_payload(switch_url)
    # 2) 요청 (재시도 간단 구현)
    sess = get_session()
    for attempt in range(3):
        try:
            resp = sess.post(
                urljoin(BASE_URL, DETAIL_POST_PATH),
                data=view_payload,
                timeout=15,
            )
            # 인코딩 지정(EUC-KR)
            resp.encoding = "euc-kr"
            parsed = parse_detail_to_schema(resp)
            # 예의상 소량 지연(서버 부하 완화)
            if attempt == 0:
                time.sleep(random.uniform(0.1, 0.3))
            return {"ok": True, "data": parsed, "url": switch_url}
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))  # 점증 백오프
    return {"ok": False, "err": str(last_err), "url": switch_url}

def parse_args():
    p = argparse.ArgumentParser(description="무료경매 차량 목록/상세 크롤러 (날짜/결과 인자)")
    p.add_argument("--start-date", required=True, help="시작일 (YYYY-MM-DD)")
    p.add_argument("--end-date",   required=True, help="종료일 (YYYY-MM-DD)")
    p.add_argument("--mode",     required=True, choices=["ongoing", "end", "total"],
                   help="경매 결과 필터: ongoing|end|total")
    p.add_argument("--save_path",  required=False, help="저장 경로 (JSON 또는 CSV)")
    p.add_argument("--workers", type=int, default=1,
                   help="동시 작업자 수 (기본: 12, 네트워크 I/O 기준으로 조정 가능)")
    return p.parse_args()

# =========================
# 메인 실행
# =========================
if __name__ == "__main__":
    args = parse_args()

    sy, sm, sd = split_ymd(args.start_date)
    ey, em, ed = split_ymd(args.end_date)

    auction_result_value = RESULT_MAP[args.mode]

    # 목록 검색 payload 구성 (요청사항: 종료일 필드 포함)
    fetch_payload = {
        "search": "ok",
        "mtype": "4",                 # 차량
        "page": "1",
        "courtNo_main": "0",
        "courtNo_sub": "99",
        "eventNo": "0",
        "eventNo2": "",
        # 시작일
        "sell_yyyy_s": sy,
        "sell_mm_s": sm,
        "sell_dd_s": sd,
        # 종료일 (신규 추가)
        "sell_yyyy_e": ey,
        "sell_mm_e": em,
        "sell_dd_e": ed,
        # 지역 전체
        "region_code1": "0",
        "region_code2": "0",
        "region_code3": "0",
        # 경매 결과
        "auction_result": auction_result_value,
        # 정렬
        "orderby": "t2.speed_usagecode|asc",
    }

    all_results = []
    error_urls = []

    try:
        # 1) 목록 URL 수집
        print("[INFO] Fetching list URLs...")
        list_urls = fetch_list_urls(get_session(), START_PAGE, END_PAGE, fetch_payload)
        print(f"[SUMMARY] collected {len(list_urls)} list URLs")
    except Exception as e:
        print(f"[ERROR] Failed to fetch list URLs: {e}")
        exit(1)

    max_workers = args.workers  # 네트워크 I/O 기준으로 적당히 조절(8~16 권장)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch_and_parse, u) for u in list_urls]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Parsing", unit="item"):
            r = f.result()
            if r["ok"]:
                all_results.append(r["data"])
            else:
                error_urls.append(r["url"])

    if error_urls:
        print(f"\n[SUMMARY] Total errors: {len(error_urls)}")
        print("Failed URLs:")
        for u in error_urls:
            print("  ", u)
            
    # 3) 파일 저장
    save_path = args.save_path or f"{args.mode}_{args.start_date}_{args.end_date}.csv"
    save_path = "result/" + save_path
    if save_path.endswith(".json"):
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
    elif save_path.endswith(".csv"):
        processed_data = []
        for item in all_results:
            cleaned_item = {k: v for k, v in item.items() if k != 'bid_history'}
            processed_data.append(cleaned_item)
        df = pd.DataFrame(processed_data)
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
    else:
        print(f"[ERROR] Unsupported save format: {args.save_path}")