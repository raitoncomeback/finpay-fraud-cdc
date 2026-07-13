"""
FinPay Fraud Feature Store API
High-performance feature serving for real-time fraud scoring
"""
import os
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUEST_COUNT = Counter('fraud_api_requests_total', 'Total API requests', ['endpoint', 'method', 'status'])
REQUEST_LATENCY = Histogram('fraud_api_request_duration_seconds', 'Request latency', ['endpoint'])
FEATURE_FRESHNESS = Gauge('fraud_api_feature_freshness_seconds', 'Seconds since feature last updated')

DB_HOST = os.getenv("RISINGWAVE_HOST", "risingwave")
DB_PORT = int(os.getenv("RISINGWAVE_PORT", "4566"))
DB_NAME = os.getenv("RISINGWAVE_DATABASE", "dev")
DB_USER = os.getenv("RISINGWAVE_USER", "root")
DB_PASSWORD = os.getenv("RISINGWAVE_PASSWORD", "")
POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

db_pool: Optional[SimpleConnectionPool] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    logger.info("Starting FinPay Fraud Feature API...")
    try:
        db_pool = SimpleConnectionPool(
            POOL_MIN, POOL_MAX,
            host=DB_HOST, port=DB_PORT,
            database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            cursor_factory=RealDictCursor
        )
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        db_pool.putconn(conn)
        logger.info("Database connection pool ready")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise
    yield
    if db_pool:
        db_pool.closeall()

app = FastAPI(
    title="FinPay Fraud Feature Store",
    description="Real-time fraud feature serving API for ML model scoring",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UserFeatures(BaseModel):
    user_id: str
    email: Optional[str] = None
    kyc_status: str = "pending"
    risk_score: int = 0
    risk_tier: str = "standard"
    account_count: int = 0
    total_balance: float = 0.0
    txn_count_7d: int = 0
    amount_sum_7d: float = 0.0
    amount_avg_7d: float = 0.0
    unique_merchants_7d: int = 0
    unique_devices_7d: int = 0
    unique_countries_7d: int = 0

class TransactionRiskScore(BaseModel):
    transaction_id: str
    user_id: str
    merchant_id: Optional[str] = None
    device_id: Optional[str] = None
    amount: float
    txn_status: str
    txn_count_7d: Optional[int] = None
    amount_sum_7d: Optional[float] = None
    amount_avg_7d: Optional[float] = None
    unique_devices_7d: Optional[int] = None
    txn_count_1h: Optional[int] = None
    amount_sum_1h: Optional[float] = None
    refund_rate_1h: Optional[float] = None
    decline_rate_1h: Optional[float] = None
    device_risk_score: Optional[int] = None
    user_risk_score: Optional[int] = None
    composite_risk_score: float = 0
    risk_tier: str = "low"

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    database: Dict[str, Any]

def get_db():
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)

@app.get("/health", response_model=HealthResponse)
async def health_check(conn=Depends(get_db)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.execute("SELECT COUNT(*) as total FROM mv_transaction_risk_score")
            risk_count = cur.fetchone()
            cur.execute("SELECT COUNT(*) as total FROM silver_user_profile")
            user_count = cur.fetchone()
        return HealthResponse(
            status="healthy",
            timestamp=datetime.utcnow().isoformat(),
            version="1.0.0",
            database={
                "status": "connected",
                "pool_size": db_pool.maxconn if db_pool else 0,
                "risk_score_count": risk_count["total"] if risk_count else 0,
                "user_profile_count": user_count["total"] if user_count else 0,
            }
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")

@app.get("/features/user/{user_id}")
async def get_user_features(user_id: str, conn=Depends(get_db)):
    start_time = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.user_id, u.email, u.kyc_status, u.risk_score, u.risk_tier,
                       u.account_count, u.total_balance,
                       v.txn_count_7d, v.amount_sum_7d, v.amount_avg_7d,
                       v.unique_merchants_7d, v.unique_devices_7d, v.unique_countries_7d
                FROM silver_user_profile u
                LEFT JOIN mv_user_velocity_7d v ON u.user_id = v.user_id
                WHERE u.user_id = %s
            """, (user_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")
        REQUEST_LATENCY.labels(endpoint="/features/user").observe(time.time() - start_time)
        REQUEST_COUNT.labels(endpoint="/features/user", method="GET", status="200").inc()
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        REQUEST_COUNT.labels(endpoint="/features/user", method="GET", status="500").inc()
        logger.error(f"Error fetching user features: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/features/transaction/{transaction_id}")
async def get_transaction_features(transaction_id: str, conn=Depends(get_db)):
    start_time = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM mv_transaction_risk_score WHERE transaction_id = %s", (transaction_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found")
        REQUEST_LATENCY.labels(endpoint="/features/transaction").observe(time.time() - start_time)
        REQUEST_COUNT.labels(endpoint="/features/transaction", method="GET", status="200").inc()
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        REQUEST_COUNT.labels(endpoint="/features/transaction", method="GET", status="500").inc()
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/features/batch")
async def get_batch_features(user_ids: list[str], conn=Depends(get_db)):
    if not user_ids:
        return {"features": [], "count": 0}
    if len(user_ids) > 1000:
        raise HTTPException(status_code=400, detail="Maximum 1000 users per batch")
    try:
        with conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(user_ids))
            cur.execute(f"""
                SELECT u.user_id, u.email, u.kyc_status, u.risk_score, u.risk_tier,
                       u.account_count, u.total_balance,
                       v.txn_count_7d, v.amount_sum_7d, v.amount_avg_7d,
                       v.unique_merchants_7d, v.unique_devices_7d, v.unique_countries_7d
                FROM silver_user_profile u
                LEFT JOIN mv_user_velocity_7d v ON u.user_id = v.user_id
                WHERE u.user_id IN ({placeholders})
            """, user_ids)
            rows = cur.fetchall()
        return {"features": [dict(row) for row in rows], "count": len(rows), "requested": len(user_ids)}
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/features/high-risk")
async def get_high_risk_transactions(
    limit: int = Query(100, le=1000),
    risk_tier: str = Query("critical", pattern="^(critical|high)$"),
    conn=Depends(get_db)
):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM mv_transaction_risk_score
                WHERE risk_tier = %s
                ORDER BY composite_risk_score DESC
                LIMIT %s
            """, (risk_tier, limit))
            rows = cur.fetchall()
        return {"transactions": [dict(row) for row in rows], "count": len(rows)}
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats")
async def api_stats(conn=Depends(get_db)):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total_transactions,
                    COUNT(*) FILTER (WHERE risk_tier = 'critical') as critical_count,
                    COUNT(*) FILTER (WHERE risk_tier = 'high') as high_count,
                    COUNT(*) FILTER (WHERE risk_tier = 'low') as low_count,
                    AVG(composite_risk_score) as avg_risk_score,
                    MAX(composite_risk_score) as max_risk_score
                FROM mv_transaction_risk_score
            """)
            stats = cur.fetchone()
            cur.execute("SELECT COUNT(*) as total_users FROM silver_user_profile")
            user_stats = cur.fetchone()
            cur.execute("SELECT COUNT(*) as total_merchants FROM silver_merchant_profile")
            merchant_stats = cur.fetchone()
        return {
            "api": "FinPay Fraud Feature Store",
            "risk_distribution": dict(stats) if stats else {},
            "user_count": user_stats["total_users"] if user_stats else 0,
            "merchant_count": merchant_stats["total_merchants"] if merchant_stats else 0,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type="text/plain")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
