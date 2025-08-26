from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
import psycopg2
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime
from zoneinfo import ZoneInfo
import time

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
    model: str
    year: int
    target_units: int

class OptimizationInput(BaseModel):
    month: str
    budget: int
    purchase_plans: List[PurchasePlan]

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

