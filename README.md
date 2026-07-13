# FinPay Fraud CDC Pipeline

> *A real-time fraud detection feature platform — because catching a stolen card in 200ms matters more than explaining it in 200 seconds.*

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.8-orange)
![Debezium](https://img.shields.io/badge/Debezium-2.7-red)
![RisingWave](https://img.shields.io/badge/RisingWave-3.0-green)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## The Problem

Payment companies process millions of transactions, but their fraud detection runs on batch queries against the same OLTP database handling checkout. Running `COUNT(*)` over the last 7 days on a busy `transactions` table locks rows, blocks writes, and takes minutes — by the time the fraud team sees the pattern, the money is gone.

The fraud team needs **sub-minute visibility** into suspicious patterns (velocity attacks, impossible travel, device anomalies) — but the database that stores transactions can't serve analytical queries without killing production performance.

---

## What FinPay Does

FinPay sits between the payment database and the fraud team. Every transaction is captured via CDC, streamed through Kafka, and materialized into real-time feature views — all without touching the production database.

```
FinPay OLTP       Debezium        Kafka         RisingWave        Fraud API
Postgres    ──→   CDC        ──→   Topics   ──→   MVs + Features ──→  FastAPI
(write-path)      (<10s)          (durable)      (real-time)        (<5ms)
```

**The 4 feature categories:**

| Category | What it catches |
|---|---|
| **Velocity** | Unusual transaction frequency (7d/30d rolling windows) |
| **Device Risk** | New devices, untrusted fingerprints, shared device abuse |
| **Merchant Risk** | High refund/decline rates, suspicious merchant categories |
| **Composite Score** | Weighted 0-100 risk score combining all signals |

Every transaction gets a `risk_tier` (low / high / critical) in real-time. The FastAPI feature store serves these at query time for ML scoring or rule-based blocking.

---

## Architecture

```
Source Layer         CDC Layer           Stream Layer         Feature Layer        Serving Layer
───────────          ─────────           ────────────         ─────────────        ─────────────
PostgreSQL    ──→    Debezium      ──→   Kafka          ──→   RisingWave     ──→   FastAPI
(OLTP, 50K/day)      (log-based)         (KRaft, durable)     (materialized MVs)   (feature API)
                                                                │
                                                                ├── silver_transactions_enriched
                                                                ├── mv_user_velocity_7d
                                                                ├── mv_user_velocity_30d
                                                                ├── mv_merchant_risk_realtime
                                                                ├── mv_device_risk_realtime
                                                                └── mv_transaction_risk_score
```

**Orchestration:** Apache Airflow (Docker Compose)
- `finpay_cdc_bootstrap` — One-time setup: schema, Debezium, RisingWave DDL
- `finpay_cdc_monitor` — Every 5 min: CDC lag, data freshness, schema drift
- `finpay_dbt_daily` — Daily: incremental dbt run + test suite

**Storage:** MinIO (S3-compatible) for future Iceberg integration

---

## Tech Stack

| Layer | Tool | Cloud equivalent |
|---|---|---|
| Source database | PostgreSQL 15 | RDS / Cloud SQL |
| CDC | Debezium 2.7 (Kafka Connect) | AWS DMS / Debezium on MSK |
| Message bus | Apache Kafka 3.8 (KRaft) | MSK / Confluent Cloud |
| Streaming SQL | RisingWave 3.0 | Apache Flink / Spark Structured Streaming |
| Transformation | dbt Core 1.8 + dbt-postgres | dbt Cloud |
| Orchestration | Apache Airflow (Docker) | Cloud Composer / MWAA |
| Feature serving | FastAPI | AWS API Gateway + Lambda |
| Object storage | MinIO | AWS S3 / GCS |

---

## Key Engineering Decisions

**Why RisingWave instead of Apache Flink?**
Flink requires Java/Scala, a JobManager + TaskManager cluster, and manual state backend tuning. RisingWave is a single binary with a PostgreSQL wire protocol — connect with `psql`, define materialized views in SQL, get real-time incremental updates. For a team that thinks in SQL, RisingWave is the pragmatic choice.

**Why CDC instead of trigger-based or batch?**
Database triggers add write latency and are fragile during schema changes. Batch ETL ( hourly/daily) is too slow for fraud detection. Log-based CDC via Debezium captures every insert/update/delete with <10s latency, zero impact on the source database, and automatic schema evolution.

**Why ephemeral dbt models instead of views?**
RisingWave doesn't support `CREATE OR REPLACE VIEW` or temporary tables. Ephemeral models compile as CTEs — they're tested by dbt but the actual data lives in RisingWave materialized views. dbt serves as the testing and documentation layer, not the runtime.

**Why composite risk score as weighted formula instead of ML?**
The rule-based `composite_risk_score` is a baseline that works without training data. It's transparent — the fraud team can see exactly why a transaction was flagged. A trained XGBoost model would replace the hardcoded weights with learned feature importance, but the feature pipeline stays the same.

---

## Pipeline Results

| Metric | Value |
|---|---|
| Synthetic transactions | 50,000 |
| CDC sources captured | 6 (users, accounts, merchants, locations, devices, transactions) |
| Silver materialized views | 4 (enriched transactions, user profile, merchant profile, device reputation) |
| Gold materialized views | 5 (velocity 7d/30d, merchant risk, device risk, composite score) |
| dbt models | 11 (4 staging + 3 intermediate + 4 marts) |
| dbt tests | 24 (all passing) |
| Feature API endpoints | 7 (health, stats, user features, transaction features, batch, high-risk, metrics) |
| Risk tiers | 3 (low / high / critical) |
| Monthly infrastructure cost | ₹0 |

---

## Running Locally

**Prerequisites:** Docker Desktop (8GB+ RAM), Python 3.11, Git

```bash
# 1. Clone and setup
git clone https://github.com/raitoncomeback/finpay-fraud-cdc.git
cd finpay-fraud-cdc
pip install -r requirements.txt

# 2. Start all services (Postgres, Kafka, MinIO, RisingWave, Airflow, Debezium)
docker-compose up -d

# 3. Initialize database and generate 50K transactions
python scripts/generate_finpay_data.py --transactions 50000

# 4. Register Debezium connector
docker cp scripts/register_debezium.sh finpay-debezium:/tmp/
docker exec finpay-debezium bash /tmp/register_debezium.sh

# 5. Create RisingWave sources and materialized views
Get-Content risingwave/ddl_bronze.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root
Get-Content risingwave/ddl_silver.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root
Get-Content risingwave/ddl_gold.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root

# 6. Run dbt transformations
cd dbt_finpay && dbt deps && dbt run && dbt test

# 7. Start the feature API
uvicorn fraud_features.api:app --host 0.0.0.0 --port 8001
# Open http://localhost:8001/docs
```

---

## Testing

**dbt tests (24/24 passing):**

| Test Type | Count | What It Catches |
|---|---|---|
| `accepted_values` | 4 | Invalid enum values (risk_tier, txn_status, kyc_status, risk_level) |
| `not_null` | 12 | Missing primary keys and required fields |
| `unique` | 8 | Duplicate records in dimension tables |

**Running tests:**
```bash
cd dbt_finpay
dbt test
# Completed successfully — PASS=24 WARN=0 ERROR=0 SKIP=0 TOTAL=24
```

---

## What the API Looks Like

The FastAPI feature store serves fraud features at query time:

```
GET /features/user/{user_id}
```
```json
{
  "user_id": "2c67caf3-5b69-4e2b-9167-e0aa54a037d8",
  "txn_count_7d": 97,
  "amount_sum_7d": 235441.25,
  "unique_devices_7d": 2,
  "unique_merchants_7d": 90,
  "risk_score": 15,
  "risk_tier": "verified"
}
```

```
GET /features/high-risk?limit=3
```
```json
{
  "transactions": [
    {
      "transaction_id": "cb9e28b9-2ef3-43f9-926d-53cf1ffe5ab9",
      "amount": 8629.62,
      "composite_risk_score": 100.0,
      "risk_tier": "critical",
      "decline_rate_30d": 0.04
    }
  ]
}
```

See [RESULTS.md](RESULTS.md) for full pipeline output: risk tier distribution, top flagged transactions, velocity users, and API response times.

---

## Deliberate Design Choices Worth Noting

**CDC captures deletes, not just inserts.** A fraudster's account being deactivated is as important as a new account being created. Debezium with `REPLICA IDENTITY FULL` captures the full before/after state of every change.

**The composite score is explainable by design.** Each weight is a named constant in the SQL — the fraud team can trace exactly why a transaction scored 100 instead of 60. Black-box ML models are great for accuracy; rule-based scores are great for compliance.

**RisingWave MVs update incrementally, not on batch.** When a new transaction hits Kafka, only the affected rows in the velocity and risk MVs are recomputed — not the full 50K row scan. This is the difference between "real-time" and "near-real-time."

**dbt tests run against RisingWave, not a separate test database.** The PostgreSQL wire protocol means dbt sees the same schema, same data, same constraints. Tests catch issues that mock-based testing would miss.

---

## Author

Built by [@raitoncomeback](https://github.com/raitoncomeback)
