# streamlit_api1_client.py
# -----------------------------------------------------------
# KCar – API1 HTTP Client (optimize)
# - /optimize 엔드포인트에 JSON을 POST하고 응답을 시각화
# - 파일 업로드/직접 붙여넣기/샘플 입력, Mock 모드 지원
# -----------------------------------------------------------

import json
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="KCar – API1 Client (optimize)", layout="wide")
st.title("KCar – API1 HTTP Client (optimize)")

# ---------------- Sidebar -----------------
with st.sidebar:
    st.header("연결 설정")
    endpoint = st.text_input("API Endpoint", value="http://43.201.57.243:8000/optimize")
    # timeout = st.number_input("Timeout (sec)", min_value=1, max_value=120, value=20, step=1)
    mock_mode = st.toggle("Mock Mode (HTTP 호출 없이 샘플 출력)", value=False)
    # 필요하면 인증 토큰 추가:
    use_auth = st.checkbox("Authorization 헤더 사용")
    token = st.text_input("Bearer Token", value="", type="password", disabled=not use_auth)

    if st.button("엔드포인트 점검"):
        try:
            t0 = time.time()
            r = requests.options(endpoint, timeout=5)
            st.success(f"OK · {r.status_code} · {time.time()-t0:.2f}s")
        except Exception as e:
            st.error(f"접속 실패: {e}")

# --------------- INPUT --------------------
st.subheader("1) 입력 JSON")

DEFAULT_INPUT = {
    "month": "2025-08-25",
    "budget": 100000000,
    "purchase_plans": [
        {"brand": "현대", "model": "아반떼", "year": 2023, "target_units": 10},
        {"brand": "기아", "model": "K5", "year": 2022, "target_units": 10}
    ]
}

col_in1, col_in2 = st.columns([2,1])
with col_in1:
    uploaded = st.file_uploader("파일 업로드(.json)", type=["json"], key="api1_input_upload")
    if uploaded is not None:
        try:
            st.session_state["input_text"] = uploaded.read().decode("utf-8")
        except Exception as e:
            st.error(f"파일 읽기 오류: {e}")

with col_in2:
    if st.button("샘플 입력 불러오기", type="secondary"):
        st.session_state["input_text"] = json.dumps(DEFAULT_INPUT, ensure_ascii=False, indent=2)

input_text = st.text_area(
    "또는 아래에 직접 붙여넣기",
    value=st.session_state.get("input_text", json.dumps(DEFAULT_INPUT, ensure_ascii=False, indent=2)),
    height=240,
)

# --------------- ACTION --------------------
c_go, c_clear = st.columns([1,1])
with c_go:
    run = st.button("요청 보내기 (POST /optimize)", type="primary")
with c_clear:
    clear = st.button("초기화")

if clear:
    for k in ["input_text", "last_request_json", "last_response_json", "last_error"]:
        st.session_state.pop(k, None)
    st.experimental_rerun()

# --------------- Helpers -------------------
def try_parse_json(txt: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(txt)
    except Exception as e:
        st.error(f"입력 JSON 파싱 오류: {e}")
        return None

def normalize_output(obj: Dict[str, Any]) -> pd.DataFrame:
    auctions = obj.get("auction_list", []) or []
    df = pd.DataFrame(auctions)
    pref = [
        "auction_house","listing_id","record_id","max_bid_price",
        "expected_price","auction_end_date","경매종료일","action_type","win_probability"
    ]
    if not df.empty:
        cols = [c for c in pref if c in df.columns] + [c for c in df.columns if c not in pref]
        df = df[cols]
    return df

def kpis_from_output(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "expected_units": obj.get("expected_purchase_units") or obj.get("expected_no_units_purchased"),
        "expected_cost": obj.get("total_expected_cost"),
        "auction_count": len(obj.get("auction_list", []) or [])
    }

# --------------- Request -------------------
if run:
    req_json = try_parse_json(input_text)
    if req_json:
        st.session_state["last_request_json"] = req_json
        if mock_mode:
            sample_out = {
              "expected_purchase_units": 3,
              "total_expected_cost": 67472110,
              "auction_list": [
                {"auction_house":"엔카오토","listing_id":"encar002","max_bid_price":16000000,"expected_price":14558400,"auction_end_date":"2025-09-02","action_type":"moderate","win_probability":0.5530},
                {"auction_house":"케이카","listing_id":"kcar003","max_bid_price":19570000,"expected_price":17730071,"auction_end_date":"2025-09-03","action_type":"moderate","win_probability":0.4823},
                {"auction_house":"오토허브","listing_id":"hub004","max_bid_price":15400000,"expected_price":15332468,"auction_end_date":"2025-09-04","action_type":"moderate","win_probability":0.4695},
                {"auction_house":"오토허브","listing_id":"hub006","max_bid_price":19150000,"expected_price":17486782,"auction_end_date":"2025-09-06","action_type":"very_aggressive","win_probability":0.8551}
              ]
            }
            st.session_state["last_response_json"] = sample_out
            st.session_state["last_error"] = None
        else:
            try:
                headers = {"Content-Type":"application/json"}
                if use_auth and token:
                    headers["Authorization"] = f"Bearer {token}"
                with st.spinner("요청 중..."):
                    t0 = time.time()
                    resp = requests.post(endpoint, json=req_json, headers=headers)
                    elapsed = time.time() - t0
                if resp.status_code >= 400:
                    st.session_state["last_error"] = f"HTTP {resp.status_code}: {resp.text[:500]}"
                    st.session_state["last_response_json"] = None
                else:
                    try:
                        st.session_state["last_response_json"] = resp.json()
                        st.session_state["last_error"] = None
                        st.toast(f"완료: {elapsed:.2f}s", icon="✅")
                    except Exception:
                        st.session_state["last_error"] = f"JSON 파싱 실패: {resp.text[:500]}"
                        st.session_state["last_response_json"] = None
            except requests.exceptions.RequestException as e:
                st.session_state["last_error"] = f"요청 실패: {e}"
                st.session_state["last_response_json"] = None

# --------------- Preview -------------------
st.markdown("---")
st.subheader("2) 요청/응답 미리보기")

c_req, c_res = st.columns(2)
with c_req:
    st.markdown("**Request JSON**")
    st.json(st.session_state.get("last_request_json") or try_parse_json(input_text) or DEFAULT_INPUT)

with c_res:
    st.markdown("**Response JSON**")
    if st.session_state.get("last_error"):
        st.error(st.session_state["last_error"])
    st.json(st.session_state.get("last_response_json") or {"hint": "아직 응답이 없습니다. (Mock Mode로도 확인 가능)"})

# --------------- KPIs & Table --------------
out_obj = st.session_state.get("last_response_json")
if out_obj:
    k = kpis_from_output(out_obj)
    c1, c2, c3 = st.columns(3)
    c1.metric("예상 매입대수", f"{(k['expected_units'] or 0):,} 대")
    total_cost = k.get("expected_cost")
    c2.metric("총 예상 비용", f"{(int(total_cost) if total_cost else 0):,} 원")
    c3.metric("경매 매물 수", f"{k['auction_count']:,} 건")

    df = normalize_output(out_obj)
    st.markdown("### 경매 매물 리스트")
    if not df.empty:
        if "max_bid_price" in df.columns:
            df["max_bid_price"] = df["max_bid_price"].map(lambda x: int(x) if pd.notna(x) else x)
        if "expected_price" in df.columns:
            df["expected_price"] = df["expected_price"].map(lambda x: int(x) if pd.notna(x) else x)

        df_fmt = df.rename(columns={
            "auction_house": "경매장",
            "listing_id": "리스팅ID",
            "record_id": "레코드ID",
            "max_bid_price": "입찰상한가(원)",
            "expected_price": "예상낙찰가(원)",
            "auction_end_date": "경매종료일",
            "경매종료일": "경매종료일",
            "action_type": "액션",
            "win_probability": "낙찰확률"
        })
        st.dataframe(df_fmt, use_container_width=True, hide_index=True)

        csv_bytes = df_fmt.to_csv(index=False).encode("utf-8-sig")
        st.download_button("CSV 다운로드", csv_bytes, file_name="api1_auction_list.csv", mime="text/csv")
    else:
        st.info("응답에 auction_list가 비어 있습니다.")
