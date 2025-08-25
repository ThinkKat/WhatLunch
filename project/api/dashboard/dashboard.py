import json
import requests
import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta

st.set_page_config(page_title="Auction Optimizer UI", layout="wide")

# ----------------------
# Helpers
# ----------------------
def post_json(url: str, payload: dict):
    try:
        resp = requests.post(url, json=payload, timeout=60)
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, {"raw": resp.text}
    except Exception as e:
        return 500, {"error": str(e)}

def _fmt_int(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return x

def _safe_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None

def _to_jsonable(v):
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d")
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, float, str, bool)):
        return v
    try:
        return int(v)
    except Exception:
        try:
            return float(v)
        except Exception:
            return str(v)

# ----------------------
# Sidebar Settings
# ----------------------
st.sidebar.header("Server Settings")
base_url = st.sidebar.text_input("Base URL", value="http://43.201.57.243:8000")
optimize_url = base_url.rstrip("/") + "/optimize"
reoptimize_url = base_url.rstrip("/") + "/reoptimize"

st.sidebar.markdown("---")
st.sidebar.caption("TIP: uvicorn fastapi_app:app --host 0.0.0.0 --port 8000")

# ----------------------
# Session storage & UI state
# ----------------------
if "api1_output" not in st.session_state:
    st.session_state["api1_output"] = None
if "api1_input" not in st.session_state:
    st.session_state["api1_input"] = None

# Expander states
if "exp_api1_input" not in st.session_state:
    st.session_state["exp_api1_input"] = True
if "exp_api1_output" not in st.session_state:
    st.session_state["exp_api1_output"] = False
if "exp_api2_prep" not in st.session_state:
    st.session_state["exp_api2_prep"] = False
if "exp_api2_request" not in st.session_state:
    st.session_state["exp_api2_request"] = False
if "exp_api2_output" not in st.session_state:
    st.session_state["exp_api2_output"] = False

st.title("Auction Optimizer (API1 → API2)")

# ======================================================
# STEP 1: API1 (/optimize)
# ======================================================
st.header("Step 1 — API1: Optimize")
st.caption("입찰상한가 계산 후 예산 내 최대 매입대수를 선정합니다. (출력 포맷: example_output.json)")

with st.expander("API1 입력 설정", expanded=st.session_state.get("exp_api1_input", True)):
    colL, colR = st.columns([2,1])
    with colL:
        default_api1 = {
            "month": str(date.today()),
            "budget": 1_000_000_000,
            "purchase_plans": [
                {"brand":"현대","model":"소나타","year":2016,"target_units":2},
                {"brand":"기아","model":"K5","year":2014,"target_units":1},
            ]
        }
        api1_text = st.text_area(
            "API1 Request Body (JSON)", 
            value=json.dumps(default_api1, ensure_ascii=False, indent=2),
            height=280
        )
        uploaded1 = st.file_uploader("또는 JSON 파일 업로드", type=["json"], key="api1_upload")
        if uploaded1 is not None:
            try:
                api1_text = uploaded1.read().decode("utf-8")
            except:
                st.error("파일을 읽을 수 없습니다.")
    with colR:
        st.info(f"POST {optimize_url}")
        run_api1 = st.button("API1 실행", type="primary")

    if run_api1:
        try:
            api1_payload = json.loads(api1_text)
        except Exception as e:
            st.error(f"JSON 파싱 실패: {e}")
            st.stop()

        st.session_state["api1_input"] = api1_payload
        code1, out1 = post_json(optimize_url, api1_payload)
        if code1 != 200:
            st.error(f"API1 실패 (HTTP {code1})")
            st.json(out1)
            st.stop()
        st.success("API1 성공")
        st.session_state["api1_output"] = out1

        st.session_state["exp_api1_input"] = False
        st.session_state["exp_api1_output"] = True
        st.session_state["exp_api2_prep"] = True
        st.session_state["exp_api2_request"] = True

api1_out = st.session_state.get("api1_output")
if api1_out:
    with st.expander("API1 결과", expanded=st.session_state.get("exp_api1_output", False)):
        st.subheader("API1 결과 미리보기")
        col1, col2, col3 = st.columns(3)
        with col1: st.metric("예상 매입대수", api1_out.get("expected_purchase_units"))
        with col2: st.metric("총 소진 금액", _fmt_int(api1_out.get("total_expected_cost", 0)))
        with col3: st.metric("목록 개수", len(api1_out.get("auction_list", [])) if isinstance(api1_out.get("auction_list"), list) else 0)
        st.dataframe(pd.json_normalize(api1_out.get("auction_list", [])), use_container_width=True)

# ======================================================
# STEP 2: API2 (/reoptimize)
# ======================================================
st.header("Step 2 — API2: Reoptimize")
st.caption("해당 월(as_of 기준)의 1일 ~ as_of-1일까지 종료된 경매 결과를 학습하여 상한가를 재조정하고, 동일 포맷으로 결과를 반환합니다.")

# prefill_block = st.container()

if api1_out and isinstance(api1_out.get("auction_list", []), list) and len(api1_out["auction_list"]) > 0:
    with st.expander("API2 결과 편집(기간/상태)", expanded=st.session_state.get("exp_api2_prep", False)):
        as_of = st.date_input("as_of (YYYY-MM-DD)", value=date.today(), help="기본: 오늘. as_of 월의 1일 ~ as_of-1일까지를 대상 기간으로 봅니다.")
        yday = as_of - timedelta(days=1)
        month_start = as_of.replace(day=1)
        st.caption(f"대상 기간: {month_start.isoformat()} ~ {yday.isoformat()}")

        al = pd.DataFrame(api1_out["auction_list"]).copy()
        al["auction_end_date"] = al["auction_end_date"].apply(_safe_date)
        period_rows = al[(al["auction_end_date"] >= month_start) & (al["auction_end_date"] <= yday)].copy()

        st.write(f"API1 결과 중 대상 기간 종료분: {period_rows.shape[0]}건")
        if period_rows.empty:
            st.info("대상 기간 종료분이 없습니다. 직접 결과를 업로드하거나 JSON을 입력해 주세요.")

        if not period_rows.empty:
            prefilled = pd.DataFrame({
                "auction_end_date": period_rows.get("auction_end_date"),
                "auction_house": period_rows.get("auction_house", ""),
                "listing_id": period_rows.get("listing_id", ""),
                "brand": period_rows.get("brand", ""),
                "model": period_rows.get("model", ""),
                "year": period_rows.get("year", None),
                "displacement_cc": period_rows.get("displacement_cc", None),
                "mileage_km": period_rows.get("mileage_km", None),
                "cap_level": period_rows.get("cap_level", ""),
                "cap_n": period_rows.get("cap_n", None),
                "max_bid_price": period_rows.get("max_bid_price", 0).astype(int),
                "result": ["NO_BID"] * len(period_rows)
            })
            st.markdown("**결과 편집 (SUCCESS / FAIL / NO_BID 지정)**")
            edited = st.data_editor(
                prefilled,
                use_container_width=True,
                num_rows="dynamic",
                key="api2_editor",
                column_config={
                    "result": st.column_config.SelectboxColumn(
                        "result",
                        help="SUCCESS / FAIL / NO_BID",
                        options=["SUCCESS","FAIL","NO_BID"],
                        required=True,
                        default="NO_BID"
                    ),
                    "auction_end_date": st.column_config.DateColumn("auction_end_date", help="경매 종료일", format="YYYY-MM-DD"),
                    "max_bid_price": st.column_config.NumberColumn("max_bid_price", help="해당 시점 상한가", step=1, min_value=0),
                    "cap_n": st.column_config.NumberColumn("cap_n")
                }
            )
        else:
            edited = pd.DataFrame(columns=["auction_house","listing_id","max_bid_price","result"])
else:
    st.info("먼저 위에서 API1을 실행하세요.")
    edited = pd.DataFrame(columns=["auction_house","listing_id","max_bid_price","result"])
    as_of = date.today()

st.markdown("---")

with st.expander("API2 요청 구성", expanded=st.session_state.get("exp_api2_request", False)):
    api1_in = st.session_state.get("api1_input") or {}
    base_budget = int(api1_in.get("budget", 0))

    c1, c2, c3 = st.columns([1.1, 1.1, 1])
    with c1:
        st.metric("기준 예산(budget, API1)", f"{base_budget:,}")
    with c2:
        auto_deduct = st.checkbox("SUCCESS 금액만큼 예산 차감", value=True,
                                  help="result=SUCCESS 행들의 max_bid_price 합계를 예산에서 미리 차감합니다. (FAIL/NO_BID는 차감 안 함)")
    if isinstance(edited, pd.DataFrame) and not edited.empty and auto_deduct:
        try:
            success_mask = edited["result"].astype(str).str.upper() == "SUCCESS"
            deduct = int(pd.to_numeric(edited.loc[success_mask, "max_bid_price"], errors="coerce").fillna(0).astype(int).sum())
        except Exception:
            deduct = 0
    else:
        deduct = 0
    with c3:
        st.metric("차감 합계", f"{deduct:,}")

    new_budget = max(0, base_budget - deduct) if auto_deduct else base_budget
    st.metric("API2 요청 예산(적용)", f"{new_budget:,}")

    auction_results_jsonable = []
    if isinstance(edited, pd.DataFrame) and not edited.empty:
        for row in edited.to_dict(orient="records"):
            auction_results_jsonable.append({k: _to_jsonable(v) for k, v in row.items()})

    api2_payload_default = {
        "budget": new_budget,
        "auction_results": auction_results_jsonable
    }

    api2_text = st.text_area(
        "API2 Request Body (JSON)",
        value=json.dumps(api2_payload_default, ensure_ascii=False, indent=2),
        height=260
    )
    uploaded2 = st.file_uploader("또는 API2 JSON 업로드", type=["json"], key="api2_upload")
    if uploaded2 is not None:
        try:
            api2_text = uploaded2.read().decode("utf-8")
        except:
            st.error("파일을 읽을 수 없습니다.")

    st.info(f"POST {reoptimize_url}?as_of={as_of.isoformat()}")
    run_api2 = st.button("API2 실행", type="primary")

    if run_api2:
        try:
            api2_payload = json.loads(api2_text)
        except Exception as e:
            st.error(f"JSON 파싱 실패: {e}")
            st.stop()

        code2, out2 = post_json(f"{reoptimize_url}?as_of={as_of.isoformat()}", api2_payload)
        if code2 != 200:
            st.error(f"API2 실패 (HTTP {code2})")
            st.json(out2)
            st.stop()
        st.success("API2 성공")
        st.session_state["exp_api2_request"] = False
        st.session_state["exp_api2_output"] = True
        st.session_state["api2_output_data"] = out2

api2_out = st.session_state.get("api2_output_data")
if api2_out:
    with st.expander("API2 결과", expanded=st.session_state.get("exp_api2_output", False)):
        st.subheader("API2 결과")
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("예상 매입대수", api2_out.get("expected_purchase_units"))
        with c2: st.metric("총 소진 금액", _fmt_int(api2_out.get("total_expected_cost", 0)))
        with c3: st.metric("목록 개수", len(api2_out.get("auction_list", [])) if isinstance(api2_out.get("auction_list"), list) else 0)
        st.dataframe(pd.json_normalize(api2_out.get("auction_list", [])), use_container_width=True)
