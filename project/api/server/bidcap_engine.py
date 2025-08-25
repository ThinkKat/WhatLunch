
import pandas as pd, numpy as np
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

@dataclass
class BidCapConfig:
    quantile: float = 0.85
    disp_bin: int = 500
    mile_bin: int = 5000
    year_tol: int = 2
    disp_tol: int = 1000
    mile_tol: int = 10000
    clip_lower: Optional[float] = 0.01
    clip_upper: Optional[float] = 0.99
    exclude_year: Optional[int] = 2025
    exclude_month: Optional[int] = 8

def _canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "brand": ["brand","브랜드","브랜드(제조사)"],
        "model": ["model","모델","모델명"],
        "year": ["year","연도","모델연도"],
        "displacement_cc": ["displacement_cc","배기량","엔진배기량"],
        "mileage_km": ["mileage_km","주행거리","주행거리(km)"],
        "winning_price": ["winning_price","낙찰가"],
        "auction_date": ["auction_date","경매종료일"],
        "auction_house": ["auction_house","경매사","경매장"],
        "listing_id": ["listing_id","매물ID","매물id","id"]
    }
    lower = {c.lower().strip(): c for c in df.columns}
    col_map = {}
    for canon, aliases in mapping.items():
        for a in aliases:
            key = a.lower().strip()
            if key in lower:
                col_map[lower[key]] = canon
                break
    return df.rename(columns=col_map)

def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["brand","model","auction_house","listing_id"]:
        if c in df.columns:
            df[c] = df[c].map(lambda x: str(x).strip() if x is not None else '')
    for c in ["year","displacement_cc","mileage_km","winning_price"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "auction_date" in df.columns:
        df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce")
    return df

def _apply_bins(df: pd.DataFrame, disp_bin: int, mile_bin: int) -> pd.DataFrame:
    df = df.copy()
    if "displacement_cc" in df.columns:
        df["disp_bin"] = np.floor(df["displacement_cc"]/disp_bin)*disp_bin
    if "mileage_km" in df.columns:
        df["mile_bin"] = np.floor(df["mileage_km"]/mile_bin)*mile_bin
    return df

def _clip_group(gr: pd.DataFrame, lo: float, hi: float) -> pd.DataFrame:
    if gr["winning_price"].notna().sum() < 2:
        return gr
    lo_v = gr["winning_price"].quantile(lo)
    hi_v = gr["winning_price"].quantile(hi)
    return gr[(gr["winning_price"]>=lo_v) & (gr["winning_price"]<=hi_v)]

def _agg_quantile(df: pd.DataFrame, keys, q: float):
    return df.groupby(keys).agg(
        bid_cap=("winning_price", lambda s: float(s.quantile(q))),
        cap_n=("winning_price","count")
    ).reset_index()

def compute_bid_caps(results_df: pd.DataFrame, schedule_df: pd.DataFrame, cfg: Optional[BidCapConfig]=None) -> pd.DataFrame:
    if cfg is None: cfg = BidCapConfig()
    results = _canonicalize(results_df.copy()); schedule = _canonicalize(schedule_df.copy())
    results = _prep(results); schedule = _prep(schedule)

    if cfg.exclude_year and cfg.exclude_month and "auction_date" in results.columns:
        mask = (results["auction_date"].dt.year==cfg.exclude_year) & (results["auction_date"].dt.month==cfg.exclude_month)
        results = results[~mask].copy()

    results = _apply_bins(results, cfg.disp_bin, cfg.mile_bin)
    schedule = _apply_bins(schedule, cfg.disp_bin, cfg.mile_bin)

    full_keys = ["brand","model","year","disp_bin","mile_bin"]
    if cfg.clip_lower is not None and cfg.clip_upper is not None:
        results = results.groupby(full_keys, group_keys=False).apply(lambda g: _clip_group(g, cfg.clip_lower, cfg.clip_upper))

    cap_full = _agg_quantile(results, full_keys, cfg.quantile)
    cap_bmy  = _agg_quantile(results, ["brand","model","year"], cfg.quantile)
    cap_bm   = _agg_quantile(results, ["brand","model"], cfg.quantile)
    cap_b    = _agg_quantile(results, ["brand"], cfg.quantile)
    global_cap = float(results["winning_price"].quantile(cfg.quantile)) if results["winning_price"].notna().any() else np.nan

    from collections import defaultdict
    by_bm = defaultdict(list)
    for _, row in cap_full.iterrows():
        by_bm[(row["brand"], row["model"])].append(row)

    def find_tol(r):
        b,m,y,d,mi = r.get("brand"), r.get("model"), r.get("year"), r.get("disp_bin"), r.get("mile_bin")
        cands = by_bm.get((b,m), [])
        best, best_score = None, None
        for cand in cands:
            if abs(cand["year"]-y)<=cfg.year_tol and abs(cand["disp_bin"]-d)<=cfg.disp_tol and abs(cand["mile_bin"]-mi)<=cfg.mile_tol:
                dist = abs(cand["year"]-y) + abs(cand["disp_bin"]-d)/cfg.disp_bin + abs(cand["mile_bin"]-mi)/cfg.mile_bin
                score = (dist, -cand["cap_n"])
                if best is None or score < best_score:
                    best, best_score = cand, score
        return best

    out = schedule.copy().reset_index(drop=True)
    out["bid_cap"] = np.nan
    out["cap_n"] = np.nan
    out["cap_level"] = ""

    for i in range(len(out)):
        r = out.loc[i, :]
        cand = find_tol(r)
        if cand is not None:
            out.loc[i, "bid_cap"] = float(cand["bid_cap"]); out.loc[i,"cap_n"]=int(cand["cap_n"]); out.loc[i,"cap_level"]="tolerance"; continue
        hit = cap_bmy[(cap_bmy["brand"]==r["brand"]) & (cap_bmy["model"]==r["model"]) & (cap_bmy["year"]==r["year"])]
        if len(hit)>0:
            out.loc[i,"bid_cap"]=float(hit["bid_cap"].iloc[0]); out.loc[i,"cap_n"]=int(hit["cap_n"].iloc[0]); out.loc[i,"cap_level"]="bmy"; continue
        hit = cap_bm[(cap_bm["brand"]==r["brand"]) & (cap_bm["model"]==r["model"])]
        if len(hit)>0:
            out.loc[i,"bid_cap"]=float(hit["bid_cap"].iloc[0]); out.loc[i,"cap_n"]=int(hit["cap_n"].iloc[0]); out.loc[i,"cap_level"]="bm"; continue
        hit = cap_b[cap_b["brand"]==r["brand"]]
        if len(hit)>0:
            out.loc[i,"bid_cap"]=float(hit["bid_cap"].iloc[0]); out.loc[i,"cap_n"]=int(hit["cap_n"].iloc[0]); out.loc[i,"cap_level"]="b"; continue
        out.loc[i,"bid_cap"]=global_cap; out.loc[i,"cap_n"]=np.nan; out.loc[i,"cap_level"]="global"

    return out
