
# purchase_allocator.py (v2 for simplified output schema)
import pandas as pd, numpy as np, json
from typing import Dict, Any, List
from collections import defaultdict

def allocate(schedule_with_caps: pd.DataFrame, plan_json: Dict[str,Any], budget: int) -> Dict[str,Any]:
    # exact match brand+model+year (no normalization), per-plan caps
    plans = pd.DataFrame(plan_json["purchase_plans"])
    plans["year"] = pd.to_numeric(plans["year"], errors="coerce")
    for c in ["brand","model"]:
        plans[c] = plans[c].astype(str).str.strip()
    sched = schedule_with_caps.copy()

    # candidate pool
    cand = sched.merge(plans, on=["brand","model","year"], how="inner")
    cand = cand[pd.notna(cand["bid_cap"])].copy()
    cand["max_bid_price"] = cand["bid_cap"].astype(float)

    # sort by cheapest max_bid_price to maximize units
    cand.sort_values("max_bid_price", inplace=True)

    remaining = budget
    taken_idx = []
    per_plan_used = defaultdict(int)

    for idx, row in cand.iterrows():
        key = (row["brand"], row["model"], int(row["year"]))
        cap_units = int(row["target_units"])
        if per_plan_used[key] >= cap_units:
            continue
        price = float(row["max_bid_price"])  # consume budget at cap
        if price <= remaining:
            remaining -= price
            per_plan_used[key] += 1
            taken_idx.append(idx)

    chosen = cand.loc[taken_idx].copy()

    def fmt_date(x):
        try:
            return pd.to_datetime(x).strftime("%Y-%m-%d")
        except Exception:
            return None

    auction_list: List[Dict[str,Any]] = []
    for _, r in chosen.iterrows():
        auction_list.append({
            "auction_house": (r["auction_house"] if "auction_house" in r and pd.notna(r["auction_house"]) else ""),
            "listing_id": (r["listing_id"] if "listing_id" in r and pd.notna(r["listing_id"]) else ""),
            "max_bid_price": int(round(float(r["max_bid_price"]))),
            "auction_end_date": fmt_date(r["auction_date"] if "auction_date" in r else None),
            # 차량 정보 추가
            "brand": r["brand"],
            "model": r["model"],
            "year": int(r["year"]) if pd.notna(r["year"]) else None,
            "displacement_cc": float(r["displacement_cc"]) if "displacement_cc" in r and pd.notna(r["displacement_cc"]) else None,
            "mileage_km": float(r["mileage_km"]) if "mileage_km" in r and pd.notna(r["mileage_km"]) else None,
            "cap_level": r["cap_level"] if "cap_level" in r else "",
            "cap_n": int(r["cap_n"]) if "cap_n" in r and pd.notna(r["cap_n"]) else None
        })

    total_expected_cost = int(sum(x["max_bid_price"] for x in auction_list))

    return {
        "expected_purchase_units": len(auction_list),
        "total_expected_cost": total_expected_cost,
        "auction_list": auction_list
    }
