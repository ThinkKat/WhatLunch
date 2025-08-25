from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
import psycopg2
import json

# Optimizer import
from optimizer import MCTSAuctionOptimizer  

# ---------------------------
# DB 연결 함수
# ---------------------------
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

def load_schedule_from_rds() -> List[Dict[str, Any]]:
    conn = get_rds_connection()
    query = "SELECT * FROM auction_schedule"
    df = pd.read_sql(query, conn)
    conn.close()
    # Optimizer에서 dict로 받으므로 변환
    return df.to_dict(orient="records")

def load_results_from_local() -> pd.DataFrame:
    return pd.read_csv("auction_results.csv")

def load_schedule_from_local() -> List[Dict[str, Any]]:
    df = pd.read_csv("auction_schedule.csv")
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

@app.post("/optimize")
def optimize_strategy(input_data: OptimizationInput):
    try:
        # 1. RDS에서 데이터 로드
        # history_df = load_results_from_local()
        # schedule_list = load_schedule_from_local()
        history_df = load_results_from_rds()
        schedule_list = load_schedule_from_rds()

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

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reoptimize")
def reoptimize_strategy(input_data: OptimizationInput):
    try:
        # 1. RDS에서 데이터 로드
        # history_df = load_results_from_local()
        # schedule_list = load_schedule_from_local()
        history_df = load_results_from_rds()
        schedule_list = load_schedule_from_rds()

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

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))