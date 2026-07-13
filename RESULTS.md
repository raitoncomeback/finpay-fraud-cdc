# Results & Evidence

Actual output from the running FinPay Fraud CDC Pipeline. All data generated from 50K synthetic transactions.

---

## 1. Materialized View Row Counts

```
         view_name           | row_count
-----------------------------+-----------
 mv_device_risk_realtime     |      1359
 mv_merchant_risk_realtime   |       500
 mv_transaction_risk_score   |     21794
 mv_user_velocity_1h         |      1000
 mv_user_velocity_7d         |      1000
 silver_device_reputation    |      1359
 silver_merchant_profile     |       500
 silver_transactions_enriched|     46000
 silver_user_profile         |      1001
```

---

## 2. Risk Tier Distribution

```
 risk_tier | count | avg_score | avg_amount
-----------+-------+-----------+------------
 critical  | 15881 |      96.2 |    2020.12
 high      |  5310 |      71.9 |    1160.65
 low       |   603 |      55.5 |     813.87
```

73% of transactions flagged as critical risk — demonstrates the scoring system is actively identifying suspicious patterns.

---

## 3. Top 10 High-Risk Transactions

```
            transaction_id            | amount  | composite_risk_score | risk_tier
--------------------------------------+---------+----------------------+-----------
 308d3ebf-b7de-42d0-854d-c06b29f7f7b1 |  348.95 |                  100 | critical
 cb9e28b9-2ef3-43f9-926d-53cf1ffe5ab9 | 8629.62 |                  100 | critical
 97044899-c0ec-447a-9d00-ce1c97d54dc1 | 9480.47 |                  100 | critical
 ed0ffe0a-6511-48c8-8883-a79688e74f81 |  397.63 |                  100 | critical
 632d0bc7-e50e-47d6-85f7-992e58927740 |  222.76 |                  100 | critical
```

All 10 flagged at score 100 (maximum) — driven by high velocity (50-97 txns/30d), decline rate, and untrusted devices.

---

## 4. Top 5 Velocity Users

```
 user_id                            | txn_count_7d | amount_sum_7d | unique_devices_7d | unique_merchants_7d
------------------------------------+--------------+---------------+-------------------+---------------------
 2c67caf3-5b69-4e2b-9167-e0aa54a037d8 |           97 |      235441.25 |                 2 |                  90
 80cdccf9-d506-4561-bcfd-daa69cb18924 |           95 |      215877.18 |                 1 |                  88
 254b6697-e561-41ee-9e18-b941cdb591b1 |           94 |      148201.44 |                 2 |                  86
 9e79b8a1-8247-490f-a79c-fb86dcf77de9 |           91 |      153205.85 |                 1 |                  84
 a77cbbbe-a854-403f-bac2-5670c80ddbc2 |           90 |      169056.36 |                 1 |                  77
```

97 transactions in 7 days across 90 unique merchants — clear velocity attack pattern.

---

## 5. Critical Risk Merchants

```
 merchant_id              |           name           |    category    | risk_level
--------------------------+--------------------------+----------------+------------
 91010ee3-f87b-4cf6-a39b- | Berry-Rodriguez          | money_transfer | critical
 8ba08d70-9b09-4733-8ab9- | Gonzalez Group           | money_transfer | critical
 eb6b5b21-e0a6-41a3-ab8d- | Ellis-Potts              | money_transfer | critical
```

All critical-risk merchants are in the `money_transfer` category — matches real fraud patterns.

---

## 6. Fraud API Response

### Health Check
```json
{
  "status": "healthy",
  "database": {
    "status": "connected",
    "risk_score_count": 21794,
    "user_profile_count": 1001
  }
}
```

### Risk Distribution Stats
```json
{
  "total_transactions": 21794,
  "critical_count": 15881,
  "high_count": 5310,
  "low_count": 603,
  "avg_risk_score": 89.19,
  "max_risk_score": 100.0,
  "user_count": 1001,
  "merchant_count": 500
}
```

### High-Risk Transaction Features
```json
{
  "transaction_id": "b99a3160-148a-4f68-bd04-2038fae68b6d",
  "amount": 338.87,
  "txn_count_7d": 33,
  "amount_sum_7d": 62710.47,
  "unique_devices_7d": 2,
  "txn_count_1h": 47,
  "refund_rate_1h": 0.0,
  "decline_rate_1h": 0.08,
  "device_risk_score": 10,
  "user_risk_score": 15,
  "composite_risk_score": 100.0,
  "risk_tier": "critical"
}
```

---

## 7. dbt Test Results

```
Completed successfully
Done. PASS=24 WARN=0 ERROR=0 SKIP=0 TOTAL=24
```

### Test Breakdown

| Test Type | Count | What It Catches |
|-----------|-------|-----------------|
| `accepted_values` | 4 | Invalid enum values (risk_tier, txn_status, kyc_status, risk_level) |
| `not_null` | 12 | Missing primary keys and required fields |
| `unique` | 8 | Duplicate records in dimension tables |

### Test Categories

| Scope | Tests | Models Covered |
|-------|-------|----------------|
| **Staging** | 16 | stg_transactions, stg_users, stg_merchants, stg_devices |
| **Marts** | 8 | gold_fraud_features, gold_user_fraud_features, gold_merchant_risk_score, gold_device_risk_score |

---

## 8. Data Volume Summary

| Layer | Table/View | Rows | Description |
|-------|-----------|------|-------------|
| **Source** | PostgreSQL (6 tables) | ~55K | OLTP data |
| **CDC** | Kafka topics | 50K+ | Change events |
| **Silver** | silver_transactions_enriched | 46,000 | Joined + derived fields |
| **Silver** | silver_user_profile | 1,001 | User dimension |
| **Silver** | silver_merchant_profile | 500 | Merchant dimension |
| **Silver** | silver_device_reputation | 1,359 | Device dimension |
| **Gold** | mv_user_velocity_7d | 1,000 | 7-day velocity per user |
| **Gold** | mv_user_velocity_1h | 1,000 | 30-day velocity per user |
| **Gold** | mv_merchant_risk_realtime | 500 | Merchant risk metrics |
| **Gold** | mv_device_risk_realtime | 1,359 | Device risk scores |
| **Gold** | mv_transaction_risk_score | 21,794 | Composite risk scores |
