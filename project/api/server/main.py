from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime
from zoneinfo import ZoneInfo
import time
from optimizer import ReoptimizationMCTSOptimizer

load_dotenv()

# Optimizer import
from optimizer import MCTSAuctionOptimizer  

# ---------------------------
# DB 연결 함수
# ---------------------------
def get_engine():
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT", "5432")

    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    return engine

def load_results_from_rds() -> pd.DataFrame:
    conn = get_engine()
    query = "SELECT * FROM auction_results"
    df = pd.read_sql(query, conn)
    return df

def load_schedule_from_rds() -> List[Dict[str, Any]]:
    conn = get_engine()
    query = "SELECT * FROM auction_schedule"
    df = pd.read_sql(query, conn)
    # Optimizer에서 dict로 받으므로 변환
    return df.to_dict(orient="records")

def load_results_from_local() -> pd.DataFrame:
    return pd.read_csv("sample_data/auction_results.csv")

def load_schedule_from_local() -> List[Dict[str, Any]]:
    df = pd.read_csv("sample_data/auction_schedule.csv")
    return df.to_dict(orient="records")

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Auction Optimization API")

# Pydantic 모델
class PurchasePlan(BaseModel):
    brand: str
    model: Optional[str] = None  # 선택적
    year: Optional[int] = None   # 선택적
    target_units: int

class OptimizationInput(BaseModel):
    month: str
    budget: int
    purchase_plans: List[PurchasePlan]

class BidResultModel(BaseModel):
    auction_house: str
    listing_id: str
    max_bid_price: int
    expected_price: int
    auction_end_date: str
    action_type: str
    win_probability: float
    actual_result: Optional[str] = None  # "won", "lost", "pending"  
    actual_price: Optional[int] = None   # Optional[int]로 변경

class ReoptimizationInputModel(BaseModel):
    month: str
    original_budget: int
    remaining_budget: int
    original_purchase_plans: List[PurchasePlan]
    remaining_targets: Dict[str, int]  # 아직 달성하지 못한 목표 {"brand_model_year": count}
    current_inventory: Dict[str, int]  # 현재까지 구매한 차량 {"brand_model_year": count}
    executed_bids: List[BidResultModel]  # 이미 실행된 입찰들

def _get_client_ip(request: Request) -> str:
    # 프록시/로드밸런서 뒤일 때 X-Forwarded-For 우선
    xff = request.headers.get("x-forwarded-for")
    if xff:
        # 가장 앞의 IP가 원래 클라이언트
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

@app.post("/optimize")
def optimize_strategy(input_data: OptimizationInput, request: Request):
    # --- 요청 메타 로그 (시각/IP) ---
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S %Z")
    ip = _get_client_ip(request)
    print(f"[REQ] {kst}  /optimize  from={ip}")

    t0 = time.time()
    try:
        # 1. RDS에서 데이터 로드
        history_df = load_results_from_local()
        schedule_list = load_schedule_from_local()
        # history_df = load_results_from_rds()
        # schedule_list = load_schedule_from_rds()

        # 2. Optimizer 초기화
        optimizer = MCTSAuctionOptimizer(history_df)

        # 3. 최적화 실행
        optimization_input = {
            "month": input_data.month,
            "budget": input_data.budget,
            "purchase_plans": [p.dict() for p in input_data.purchase_plans],
            "auction_schedule": schedule_list
        }

        result = optimizer.optimize_auction_strategy(
            optimization_input=optimization_input,
            iterations=5000
        )

        # --- 성공 로그(처리시간) ---
        dt = time.time() - t0
        print(f"[OK ] {kst}  /optimize  from={ip}  took={dt:.2f}s")

        return result

    except Exception as e:
        # --- 실패 로그(에러 요약 + 처리시간) ---
        dt = time.time() - t0
        print(f"[ERR] {kst}  /optimize  from={ip}  took={dt:.2f}s  err={type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/reoptimize")
def reoptimize_strategy(input_data: ReoptimizationInputModel, request: Request):
    # --- 요청 메타 로그 (시각/IP) ---
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S %Z")
    ip = _get_client_ip(request)
    print(f"[REQ] {kst}  /reoptimize  from={ip}")

    t0 = time.time()
    try:
        # 1. RDS에서 데이터 로드
        history_df = load_results_from_local()
        schedule_list = load_schedule_from_local()
        # history_df = load_results_from_rds()  
        # schedule_list = load_schedule_from_rds()

        # 2. 재최적화용 Optimizer 초기화
        reoptimizer = ReoptimizationMCTSOptimizer(history_df)

        # 3. 입력 데이터 변환
        executed_bids = [
            BidResultModel(
                auction_house=bid.auction_house,
                listing_id=bid.listing_id,
                max_bid_price=bid.max_bid_price,
                expected_price=bid.expected_price,
                auction_end_date=bid.auction_end_date,
                action_type=bid.action_type,
                win_probability=bid.win_probability,
                actual_result=bid.actual_result,
                actual_price=bid.actual_price
            )
            for bid in input_data.executed_bids
        ]

        # 4. 남은 경매 일정 필터링 (이미 실행된 입찰 제외)
        executed_listing_ids = {bid.listing_id for bid in input_data.executed_bids}
        remaining_schedule = [
            auction for auction in schedule_list 
            if auction.get('listing_id') not in executed_listing_ids
        ]

        # 5. ReoptimizationInput 생성
        reopt_input = ReoptimizationInputModel(
            month=input_data.month,
            original_budget=input_data.original_budget,
            remaining_budget=input_data.remaining_budget,
            original_purchase_plans=[p.dict() for p in input_data.original_purchase_plans],
            remaining_targets=input_data.remaining_targets,
            current_inventory=input_data.current_inventory,
            executed_bids=executed_bids,
            remaining_auction_schedule=remaining_schedule
        )

        # 6. 재최적화 실행
        result = reoptimizer.reoptimize_strategy(
            reopt_input=reopt_input,
            iterations=3000  # 재최적화는 좀 더 적은 반복으로
        )

        # --- 성공 로그(처리시간) ---
        dt = time.time() - t0
        print(f"[OK ] {kst}  /reoptimize  from={ip}  took={dt:.2f}s")

        return result

    except Exception as e:
        # --- 실패 로그(에러 요약 + 처리시간) ---
        dt = time.time() - t0
        print(f"[ERR] {kst}  /reoptimize  from={ip}  took={dt:.2f}s  err={type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

    