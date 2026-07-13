# FinPay Fraud CDC Pipeline

> **Real-time fraud detection feature platform** built with CDC (Change Data Capture), RisingWave streaming SQL, and dbt.

## Project Story

FinPay processes 50K+ transactions/day. The fraud team needs **sub-minute visibility** into suspicious patterns (velocity attacks, impossible travel, geo anomalies) but the OLTP Postgres can't serve analytical queries without locking up checkout.

**Solution:** Built a **CDC → Streaming SQL → Feature Store** pipeline:
- **Debezium** captures every transaction insert/update from Postgres → Kafka (<10s latency)
- **RisingWave** creates real-time materialized views for fraud features (velocity, geo, device, merchant risk)
- **dbt** models transform raw CDC → enriched fraud features with data quality tests
- **FastAPI** serves features at <50ms for real-time ML scoring

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
git clone https://github.com/yourusername/finpay-fraud-cdc.git
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
- `mv_user_velocity_1h` - 1-hour rolling velocity (real-time alerts)
- `mv_merchant_risk_realtime` - Merchant refund/decline rates (1h window)
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

## License

MIT License
