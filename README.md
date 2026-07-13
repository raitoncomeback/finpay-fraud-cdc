# FinPay Fraud CDC Pipeline

> **Real-time fraud detection feature platform** built with CDC (Change Data Capture), RisingWave streaming SQL, and dbt.

## Project Story

FinPay processes 50K+ transactions/day. The fraud team needs **sub-minute visibility** into suspicious patterns (velocity attacks, impossible travel, geo anomalies) but the OLTP Postgres can't serve analytical queries without locking up checkout.

**Solution:** Built a **CDC → Streaming SQL → Feature Store** pipeline:
- **Debezium** captures every transaction insert/update from Postgres → Kafka (<10s latency)
- **RisingWave** creates real-time materialized views for fraud features (velocity, geo, device, merchant risk)
- **dbt** models transform raw CDC → enriched fraud features with data quality tests
- **FastAPI** serves features at <50ms for real-time ML scoring

> **ML Integration:** The 25+ features served by the API (velocity, device reputation, merchant risk, composite score) are designed to feed an XGBoost/LightGBM fraud classifier. The `composite_risk_score` is a rule-based baseline; a trained ML model would replace this with learned feature weights.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│  FinPay     │     │   Debezium  │     │   Apache Kafka  │     │     RisingWave       │
│  Postgres   │────▶│   Connect   │────▶│  (CDC Topics)   │────▶│  Streaming SQL       │
│  (OLTP)     │     │  (2.7)      │     │  (KRaft mode)   │     │  Materialized Views  │
└─────────────┘     └─────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                                           │
                                                            ┌──────────────┴───────────┐
                                                            │                          │
                                                            ▼                          ▼
                                                   ┌──────────────┐          ┌──────────────────┐
                                                   │   dbt Models │          │  FastAPI Feature  │
                                                   │  (ephemeral) │          │  Store API        │
                                                   │  24 tests    │          │  <50ms p99        │
                                                   └──────────────┘          └──────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|------------|-----|
| **CDC** | Debezium 2.7 | Log-based, exactly-once, schema evolution |
| **Message Bus** | Apache Kafka 3.8 (KRaft) | Durable, ordered, replayable |
| **Streaming SQL** | RisingWave 3.0 | PostgreSQL-compatible, real-time materialized views |
| **Transform** | dbt 1.8 + dbt-postgres | Modular SQL, testing, documentation |
| **Orchestration** | Apache Airflow | DAGs for bootstrap, daily runs, monitoring |
| **Feature Serving** | FastAPI | <50ms p99, PostgreSQL wire protocol |
| **Source DB** | PostgreSQL 15 | OLTP with CDC publication |
| **Object Storage** | MinIO | S3-compatible, bucket setup for Iceberg |

---

## Quick Start

### Prerequisites
- Docker Desktop with 8GB+ RAM
- Git
- Python 3.11 (for local dbt)

### 1. Clone & Start Infrastructure
```bash
git clone https://github.com/raitoncomeback/finpay-fraud-cdc.git
cd finpay-fraud-cdc

# Start all services (Postgres, Kafka, MinIO, RisingWave, Airflow, Debezium)
docker-compose up -d

# Verify all containers are running
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr finpay
```

### 2. Generate Synthetic Data
```bash
# Install Python dependencies
pip install -r requirements.txt

# Generate 50K transactions with embedded fraud patterns
python scripts/generate_finpay_data.py --transactions 50000
```

### 3. Register Debezium Connector
```bash
# Copy registration script to container and run
docker cp scripts/register_debezium.sh finpay-debezium:/tmp/
docker exec finpay-debezium bash /tmp/register_debezium.sh

# Verify in Kafka UI: http://localhost:8090
```

### 4. Create RisingWave Sources & Materialized Views
```powershell
# Run bronze DDL (Kafka sources)
Get-Content risingwave/ddl_bronze.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root

# Run silver DDL (enriched materialized views)
Get-Content risingwave/ddl_silver.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root

# Run gold DDL (fraud feature materialized views)
Get-Content risingwave/ddl_gold.sql | docker run --rm -i --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root
```

### 5. Run dbt Transformations
```bash
cd dbt_finpay
dbt deps
dbt run
dbt test
```

### 6. Verify Everything Works
```powershell
# Check RisingWave data
docker run --rm --network finpay-fraud-cdc_datagate-net postgres:15-alpine psql -h risingwave -p 4566 -d dev -U root -c "SELECT 'txns' AS tbl, COUNT(*) FROM silver_transactions_enriched UNION ALL SELECT 'users', COUNT(*) FROM silver_user_profile UNION ALL SELECT 'risk_scores', COUNT(*) FROM mv_transaction_risk_score;"

# Check Fraud API
(Invoke-WebRequest -Uri http://localhost:8001/health -UseBasicParsing).Content
```

---

## Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8090 | - |
| **MinIO Console** | http://localhost:9001 | `datagate` / `datagate123` |
| **Airflow** | http://localhost:8080 | `admin` / `admin` |
| **Fraud API Docs** | http://localhost:8001/docs | - |
| **Fraud API Health** | http://localhost:8001/health | - |

---

## Data Layers

### Silver Layer (RisingWave Materialized Views)
- `silver_transactions_enriched` - Transactions joined with users, merchants, locations, devices
- `silver_user_profile` - User profiles with risk tier and account summary
- `silver_merchant_profile` - Merchant profiles with risk level
- `silver_device_reputation` - Device reputation scores

### Gold Layer (Real-Time Fraud Features)
- `mv_user_velocity_7d` - 7-day rolling transaction velocity per user
- `mv_user_velocity_30d` - 30-day velocity per user (real-time alerts)
- `mv_merchant_risk_realtime` - Merchant refund/decline rates (30-day window)
- `mv_device_risk_realtime` - Device risk scoring
- `mv_transaction_risk_score` - Composite risk score (0-100) per transaction

### Fraud Features (FastAPI)
| Endpoint | Description |
|----------|-------------|
| `GET /features/user/{user_id}` | User-level fraud features |
| `GET /features/transaction/{id}` | Transaction risk score |
| `POST /features/batch` | Batch user features (up to 1000) |
| `GET /features/high-risk` | Recent high-risk transactions |
| `GET /stats` | Risk distribution stats |

---

## Project Structure

```
finpay-fraud-cdc/
├── docker-compose.yml              # Full local stack (12 services)
├── risingwave/
│   ├── ddl_bronze.sql              # Kafka sources (6 tables)
│   ├── ddl_silver.sql              # Enriched MVs (4 views)
│   ├── ddl_gold.sql                # Fraud feature MVs (5 views)
│   └── risingwave.toml             # Compactor memory config
├── dbt_finpay/
│   ├── dbt_project.yml
│   ├── profiles.yml                # PostgreSQL adapter → RisingWave
│   └── models/
│       ├── staging/                # Source mappings + schema tests
│       ├── intermediate/           # Velocity, merchant risk, device reputation
│       └── marts/                  # Gold layer fraud features
├── fraud_features/
│   ├── api.py                      # FastAPI feature serving
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   ├── init_finpay.sql             # Postgres schema + CDC publication
│   ├── generate_finpay_data.py     # 50K synthetic transactions
│   └── register_debezium.sh        # Debezium connector registration
├── airflow/dags/
│   ├── finpay_cdc_bootstrap.py     # One-time setup DAG
│   ├── finpay_cdc_monitor.py       # 5-min monitoring
│   └── finpay_dbt_daily.py         # Daily dbt run
├── .github/workflows/
│   └── ci-cd.yml                   # CI/CD pipeline
├── Makefile
└── README.md
```

---

## Why RisingWave over Flink/Spark

| Criteria | RisingWave | Apache Flink | Spark Structured Streaming |
|----------|-----------|--------------|---------------------------|
| **Learning curve** | SQL-only (PG wire protocol) | Java/Scala + SQL | Scala + SQL |
| **State backend** | Managed internally | Manual tuning (RocksDB) | Manual tuning |
| **Materialized views** | Built-in, incremental | Requires Flink SQL + catalog | Micro-batch only |
| **Deployment** | Single binary | JobManager + TaskManager | Spark cluster |
| **Latency** | Sub-second | Sub-second | Micro-batch (seconds) |

RisingWave was chosen for its simplicity: connect with any PostgreSQL client, define materialized views in SQL, and get real-time incremental updates without managing state or clusters.

---

## SQL Walkthrough: Key Window Functions

### Velocity Score (`ddl_gold.sql`)
```sql
-- 7-day rolling transaction count per user
COUNT(*) OVER (
    PARTITION BY user_id
    ORDER BY initiated_at
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
) AS txn_count_7d
```
This counts all transactions per user within a 7-day sliding window, updating in real-time as new transactions arrive.

### Composite Risk Score (`ddl_gold.sql`)
```sql
LEAST(100,
    COALESCE(txn_count_30d, 0) * 2 +           -- velocity weight
    COALESCE(refund_rate_30d, 0) * 50 +         -- refund fraud signal
    COALESCE(decline_rate_30d, 0) * 30 +        -- decline fraud signal
    COALESCE(device_risk_score, 10) / 2 +       -- device trust
    COALESCE(user_risk_score, 0) / 2 +          -- user history
    CASE WHEN amount > 5000 THEN 10 ELSE 0 END + -- high amount
    CASE WHEN NOT card_present THEN 5 ELSE 0 END -- card-not-present
) AS composite_risk_score
```
Weighted formula combining velocity, merchant risk, device trust, and user profile into a 0-100 score.

---

## dbt Tests (24 Tests)

| Test Type | Count | What It Catches |
|-----------|-------|-----------------|
| `accepted_values` | 4 | Invalid enum values (risk_tier, txn_status, kyc_status, risk_level) |
| `not_null` | 12 | Missing primary keys and required fields |
| `unique` | 8 | Duplicate records in dimension tables |

### Debugging Failures
1. Run `dbt test` to see which test fails
2. Check the compiled SQL in `target/` for the failing test
3. Query the source table to inspect the bad data
4. Fix in staging model or source data, re-run

---

## Results

See [RESULTS.md](RESULTS.md) for actual output: risk tier distribution, top flagged transactions, velocity users, API responses, and dbt test results.

---

## License

MIT License
