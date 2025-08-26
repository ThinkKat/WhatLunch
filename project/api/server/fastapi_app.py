
from fastapi import FastAPI, HTTPException, Body
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import os
import pandas as pd
import sys
import psycopg2
from dotenv import load_dotenv
load_dotenv()

sys.path.append(".")

def get_rds_connection():
    conn = psycopg2.connect(
        host="your-rds-endpoint.amazonaws.com",
        dbname="your-dbname",
        user="your-username",
        password="your-password",
        port=5432
    )
    return conn

def load_results_from_rds() -> pd.DataFrame:
    conn = get_rds_connection()
    query = "SELECT * FROM auction_results"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_schedule_from_rds() -> pd.DataFrame:
    conn = get_rds_connection()
    query = "SELECT * FROM auction_schedule"
    df = pd.read_sql(query, conn)
    conn.close()
    # Optimizer에서 dict로 받으므로 변환
    return df

def load_results_from_local() -> pd.DataFrame:
    return pd.read_csv("auction_results.csv")

def load_schedule_from_local() -> List[Dict[str, Any]]:
    df = pd.read_csv("auction_schedule.csv")
    return df.to_dict(orient="records")

# Asia/Seoul
TRY_TZ = timezone(timedelta(hours=9))

from bidcap_engine import compute_bid_caps, BidCapConfig
from purchase_allocator import allocate

app = FastAPI(title="Auction Optimizer API", version="1.1.0")

def _load_dataframes() -> Dict[str, pd.DataFrame]:
    results_path = os.environ.get("RESULTS_PATH", "auction_results.csv")
    schedule_path = os.environ.get("SCHEDULE_PATH", "automart-20250822-rds.csv")
    try:
        results_df = pd.read_csv(results_path, encoding="utf-8-sig")
        schedule_df = pd.read_csv(schedule_path, encoding="utf-8-sig")
    except FileNotFoundError:
        results_df = load_results_from_rds()
        schedule_df = load_schedule_from_rds()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load CSV data: {e}")
    return {"results": results_df, "schedule": schedule_df}

def _normalize_request(payload: Dict[str, Any]) -> (Dict[str, Any], Dict[str, Any]):
    """
    Return (optimization_input, options)
    Accepts:
      A) {"optimization_input": {...}, "options": {...}}
      B) {"month":..., "budget":..., "purchase_plans":[...], "options": {...}}
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    options = payload.get("options", {}) if isinstance(payload.get("options", {}), dict) else {}

    if "optimization_input" in payload and isinstance(payload["optimization_input"], dict):
        opt_input = payload["optimization_input"]
    else:
        # Unwrapped format -> treat payload itself as optimization_input
        opt_input = {k: v for k, v in payload.items() if k in ("month","budget","purchase_plans")}

    # Validate minimal fields
    if "budget" not in opt_input:
        raise HTTPException(status_code=422, detail="Missing 'budget' in request")
    if "purchase_plans" not in opt_input or not isinstance(opt_input["purchase_plans"], list):
        raise HTTPException(status_code=422, detail="Missing or invalid 'purchase_plans' list in request")

    # Coerce year/target_units
    plans_norm: List[Dict[str, Any]] = []
    for i, item in enumerate(opt_input["purchase_plans"]):
        if not isinstance(item, dict):
            raise HTTPException(status_code=422, detail=f"purchase_plans[{i}] must be an object")
        try:
            plans_norm.append({
                "brand": str(item.get("brand","")).strip(),
                "model": str(item.get("model","")).strip(),
                "year": int(item.get("year")) if item.get("year") is not None else None,
                "target_units": int(item.get("target_units", 0)),
            })
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"purchase_plans[{i}] invalid types: {e}")

    opt_input_norm = {
        "month": opt_input.get("month"),
        "budget": int(opt_input["budget"]),
        "purchase_plans": plans_norm,
    }
    return opt_input_norm, options

def _cfg_from_options(options: Dict[str, Any]) -> BidCapConfig:
    def _get(k, default): return options.get(k, default)
    return BidCapConfig(
        quantile=float(_get("quantile", 0.85)),
        disp_bin=int(_get("disp_bin", 500)),
        mile_bin=int(_get("mile_bin", 5000)),
        year_tol=int(_get("year_tol", 2)),
        disp_tol=int(_get("disp_tol", 1000)),
        mile_tol=int(_get("mile_tol", 10000)),
        clip_lower=float(_get("clip_lower", 0.01)) if _get("clip_lower", 0.01) is not None else None,
        clip_upper=float(_get("clip_upper", 0.99)) if _get("clip_upper", 0.99) is not None else None,
        exclude_year=int(_get("exclude_year", 2025)) if _get("exclude_year", 2025) is not None else None,
        exclude_month=int(_get("exclude_month", 8)) if _get("exclude_month", 8) is not None else None,
    )

@app.post("/optimize")
def post_optimize(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    opt_input, options = _normalize_request(payload)
    cfg = _cfg_from_options(options)

    dfs = _load_dataframes()
    results_df, schedule_df = dfs["results"], dfs["schedule"]

    # 1) Compute bid caps
    sched_with_caps = compute_bid_caps(results_df, schedule_df, cfg)

    # 2) Allocate
    out = allocate(sched_with_caps, opt_input, int(opt_input["budget"]))
    return out



from datetime import datetime, timedelta, timezone

TRY_TZ = timezone(timedelta(hours=9))  # Asia/Seoul (UTC+9)

def _to_date(dstr: str) -> datetime.date:
    return pd.to_datetime(dstr).date()

def _calc_adjust_factor(sr: float) -> float:
    # Map success-rate -> multiplicative adjustment of caps
    # Bounds later applied to +/- 15%
    if sr < 0.30:   return 0.10   # +10%
    if sr < 0.50:   return 0.05   # +5%
    if sr < 0.70:   return 0.02   # +2%
    if sr <= 0.85:  return 0.00   # keep
    return -0.05                    # -5% if too easy

# --- /reoptimize: API1(/optimize)와 동일한 출력 스키마 반환 ---
@app.post("/reoptimize")
def post_reoptimize(payload: Dict[str, Any] = Body(...), as_of: Optional[str] = None) -> Dict[str, Any]:
    """
    어제(= as_of - 1일) 경매 결과 피드백을 학습해, 미래 매물의 상한가를 조정하고
    /optimize 와 동일한 포맷으로 결과를 반환한다.
    입력 스키마:
      {
        "budget": <int>,
        "auction_results": [
          {"auction_house": "...", "listing_id": "...", "max_bid_price": <int>, "result": "SUCCESS"|"FAIL"}
        ]
      }
    쿼리 파라미터:
      as_of=YYYY-MM-DD (없으면 Asia/Seoul 기준 오늘)
    """
    # ---- 입력 검증 ----
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be an object")
    if "auction_results" not in payload or not isinstance(payload["auction_results"], list):
        raise HTTPException(status_code=422, detail="Missing 'auction_results' (list)")
    budget = int(payload.get("budget", 0))

    # ---- 기준일자 계산 (어제) ----
    if as_of:
        as_of_date = pd.to_datetime(as_of).date()
    else:
        as_of_date = datetime.now(TRY_TZ).date()
    yday = as_of_date - timedelta(days=1)

    # ---- 데이터 로드 & baseline 상한가 계산 ----
    dfs = _load_dataframes()
    results_df, schedule_df = dfs["results"], dfs["schedule"]
    cfg = BidCapConfig()  # q=0.85, bin 500/5000, tol 연식±2/배기±1000/주행±10000, 1~99% 클리핑, (2025-08) 제외
    sched_caps = compute_bid_caps(results_df, schedule_df, cfg)

    # ---- 어제 결과 조인 (auction_house, listing_id) ----
    fb = pd.DataFrame(payload["auction_results"])
    for c in ["auction_house", "listing_id", "result"]:
        if c in fb.columns:
            fb[c] = fb[c].astype(str).str.strip()
    if "max_bid_price" in fb.columns:
        fb["max_bid_price"] = pd.to_numeric(fb["max_bid_price"], errors="coerce")

    # 필요한 컬럼 보강
    for c in ["auction_house","listing_id","auction_date","brand","model","year",
              "bid_cap","displacement_cc","mileage_km","cap_level","cap_n"]:
        if c not in sched_caps.columns:
            sched_caps[c] = None

    sched_caps["auction_date"] = pd.to_datetime(sched_caps["auction_date"], errors="coerce")
    yday_rows = sched_caps[sched_caps["auction_date"].dt.date == yday].copy()

    joined = yday_rows.merge(
        fb, on=["auction_house","listing_id"], how="inner", suffixes=("_sched","_fb")
    )

    # ---- 그룹별 성공률 → 조정계수 산출 ----
    def _calc_adjust_factor(sr: float) -> float:
        # 안전범위는 이후 ±0.15로 클리핑
        if sr < 0.30:   return 0.10
        if sr < 0.50:   return 0.05
        if sr < 0.70:   return 0.02
        if sr <= 0.85:  return 0.00
        return -0.05

    grp = None
    if not joined.empty:
        joined["is_success"] = (joined["result"].str.upper() == "SUCCESS").astype(int)
        grp = joined.groupby(["brand","model","year"]).agg(
            count=("is_success","size"),
            success=("is_success","sum")
        ).reset_index()
        grp["success_rate"]  = grp["success"] / grp["count"]
        grp["adjust_factor"] = grp["success_rate"].map(_calc_adjust_factor).clip(-0.15, 0.15)

    # ---- 미래 매물 상한가 조정 ----
    future_mask = sched_caps["auction_date"].dt.date >= as_of_date
    fut = sched_caps[future_mask].copy()

    if grp is not None and not grp.empty:
        fut = fut.merge(grp[["brand","model","year","adjust_factor"]],
                        on=["brand","model","year"], how="left")
    else:
        fut["adjust_factor"] = 0.0

    fut["adjust_factor"] = fut["adjust_factor"].fillna(0.0)
    fut["adj_bid_cap"]   = (fut["bid_cap"].astype(float) * (1.0 + fut["adjust_factor"])).round()

    # ---- 예산 내 최대 대수(그리디) 선택 (계획 제한 없음) ----
    fut = fut.sort_values("adj_bid_cap")
    remaining = budget
    chosen_idx = []
    for idx, row in fut.iterrows():
        price = float(row["adj_bid_cap"]) if pd.notna(row["adj_bid_cap"]) else None
        if price is None or price <= 0:
            continue
        if price <= remaining:
            remaining -= price
            chosen_idx.append(idx)

    chosen = fut.loc[chosen_idx].copy()

    # ---- API1과 동일한 출력 스키마 구성 ----
    def fmt_date(x):
        try:
            return pd.to_datetime(x).strftime("%Y-%m-%d")
        except Exception:
            return None

    auction_list = []
    for _, r in chosen.iterrows():
        auction_list.append({
            "auction_end_date": fmt_date(r["auction_date"]),
            "auction_house":    str(r["auction_house"]) if pd.notna(r["auction_house"]) else "",
            "brand":            str(r["brand"]) if pd.notna(r["brand"]) else "",
            "cap_level":        str(r["cap_level"]) if pd.notna(r["cap_level"]) else "",
            "cap_n":            int(r["cap_n"]) if pd.notna(r["cap_n"]) else None,
            "displacement_cc":  float(r["displacement_cc"]) if pd.notna(r["displacement_cc"]) else None,
            "listing_id":       str(r["listing_id"]) if pd.notna(r["listing_id"]) else "",
            "max_bid_price":    int(round(float(r["adj_bid_cap"]))),
            "mileage_km":       float(r["mileage_km"]) if pd.notna(r["mileage_km"]) else None,
            "model":            str(r["model"]) if pd.notna(r["model"]) else "",
            "year":             int(r["year"]) if pd.notna(r["year"]) else None
        })

    total_cost = int(sum(item["max_bid_price"] for item in auction_list))
    return {
        "expected_purchase_units": len(auction_list),
        "total_expected_cost": total_cost,
        "auction_list": auction_list
    }