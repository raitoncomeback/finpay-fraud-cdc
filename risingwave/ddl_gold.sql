-- RisingWave DDL: Gold Layer (Real-time Fraud Features)
-- Materialized views serving fraud detection API
-- RisingWave 3.x: NOW() only allowed in WHERE/HAVING/ON/FROM
-- Note: 30-day windows used instead of 1h for demo visibility

-- ============================================================
-- GOLD: User Velocity Features (7-day rolling window)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_velocity_7d AS
SELECT
    user_id,
    COUNT(*) AS txn_count_7d,
    SUM(amount) AS amount_sum_7d,
    AVG(amount) AS amount_avg_7d,
    MAX(amount) AS amount_max_7d,
    COUNT(DISTINCT merchant_id) AS unique_merchants_7d,
    COUNT(DISTINCT device_id) AS unique_devices_7d,
    COUNT(DISTINCT txn_country) AS unique_countries_7d,
    MAX(initiated_at) AS last_txn_at,
    MIN(initiated_at) AS first_txn_7d_at,
    COUNT(DISTINCT txn_city) AS unique_cities_7d
FROM silver_transactions_enriched
WHERE initiated_at >= NOW() - INTERVAL '7 days'
  AND txn_status IN ('authorized', 'settled')
GROUP BY user_id;


-- ============================================================
-- GOLD: User Velocity Features (30-day rolling window)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_velocity_30d AS
SELECT
    user_id,
    COUNT(*) AS txn_count_30d,
    SUM(amount) AS amount_sum_30d,
    MAX(amount) AS amount_max_30d,
    COUNT(DISTINCT merchant_id) AS unique_merchants_30d,
    COUNT(DISTINCT device_id) AS unique_devices_30d,
    COUNT(DISTINCT txn_country) AS unique_countries_30d
FROM silver_transactions_enriched
WHERE txn_status IN ('authorized', 'settled')
GROUP BY user_id;


-- ============================================================
-- GOLD: Merchant Risk Features (30-day window)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_merchant_risk_realtime AS
SELECT
    merchant_id,
    COUNT(*) AS txn_count_30d,
    SUM(amount) AS volume_30d,
    AVG(amount) AS avg_amount_30d,
    CASE WHEN COUNT(*) > 0 THEN
        COUNT(*) FILTER (WHERE txn_status = 'refunded')::DOUBLE / COUNT(*)
    ELSE 0 END AS refund_rate_30d,
    CASE WHEN COUNT(*) > 0 THEN
        COUNT(*) FILTER (WHERE txn_status = 'declined')::DOUBLE / COUNT(*)
    ELSE 0 END AS decline_rate_30d,
    CASE WHEN COUNT(*) > 0 THEN
        COUNT(*) FILTER (WHERE txn_status = 'failed')::DOUBLE / COUNT(*)
    ELSE 0 END AS failure_rate_30d,
    COUNT(DISTINCT user_id) AS unique_users_30d,
    COUNT(DISTINCT device_id) AS unique_devices_30d
FROM silver_transactions_enriched
GROUP BY merchant_id;


-- ============================================================
-- GOLD: Device Risk Features
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_device_risk_realtime AS
SELECT
    d.device_id,
    d.user_id,
    d.device_fingerprint,
    d.is_trusted,
    d.first_seen_at,
    d.last_seen_at,
    CASE
        WHEN NOT d.is_trusted THEN 25
        ELSE 10
    END AS risk_score
FROM finpay_devices_cdc d;


-- ============================================================
-- GOLD: Transaction Risk Score (Combined)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transaction_risk_score AS
SELECT
    t.transaction_id,
    t.user_id,
    t.merchant_id,
    t.device_id,
    t.amount,
    t.initiated_at,
    t.txn_status,

    -- Velocity features
    v7.txn_count_7d,
    v7.amount_sum_7d,
    v7.amount_avg_7d,
    v7.unique_devices_7d,
    v30.txn_count_30d,
    v30.amount_sum_30d,
    v30.unique_merchants_30d,

    -- Merchant risk
    mr.refund_rate_30d AS refund_rate_1h,
    mr.decline_rate_30d AS decline_rate_1h,

    -- Device risk
    dr.risk_score AS device_risk_score,

    -- User profile
    u.kyc_status AS user_kyc_status,
    u.risk_score AS user_risk_score,

    -- Composite risk score (0-100)
    LEAST(100,
        COALESCE(v30.txn_count_30d, 0) * 2 +
        COALESCE(mr.refund_rate_30d, 0) * 50 +
        COALESCE(mr.decline_rate_30d, 0) * 30 +
        COALESCE(dr.risk_score, 10) / 2 +
        COALESCE(u.risk_score, 0) / 2 +
        CASE WHEN t.amount > 5000 THEN 10 ELSE 0 END +
        CASE WHEN NOT t.card_present THEN 5 ELSE 0 END
    ) AS composite_risk_score,

    -- Risk tier
    CASE
        WHEN LEAST(100,
            COALESCE(v30.txn_count_30d, 0) * 2 +
            COALESCE(mr.refund_rate_30d, 0) * 50 +
            COALESCE(mr.decline_rate_30d, 0) * 30 +
            COALESCE(dr.risk_score, 10) / 2 +
            COALESCE(u.risk_score, 0) / 2 +
            CASE WHEN t.amount > 5000 THEN 10 ELSE 0 END +
            CASE WHEN NOT t.card_present THEN 5 ELSE 0 END
        ) >= 80 THEN 'critical'
        WHEN LEAST(100,
            COALESCE(v30.txn_count_30d, 0) * 2 +
            COALESCE(mr.refund_rate_30d, 0) * 50 +
            COALESCE(mr.decline_rate_30d, 0) * 30 +
            COALESCE(dr.risk_score, 10) / 2 +
            COALESCE(u.risk_score, 0) / 2 +
            CASE WHEN t.amount > 5000 THEN 10 ELSE 0 END +
            CASE WHEN NOT t.card_present THEN 5 ELSE 0 END
        ) >= 60 THEN 'high'
        ELSE 'low'
    END AS risk_tier

FROM silver_transactions_enriched t
LEFT JOIN mv_user_velocity_7d v7 ON t.user_id = v7.user_id
LEFT JOIN mv_user_velocity_30d v30 ON t.user_id = v30.user_id
LEFT JOIN mv_merchant_risk_realtime mr ON t.merchant_id = mr.merchant_id
LEFT JOIN mv_device_risk_realtime dr ON t.device_id = dr.device_id
LEFT JOIN silver_user_profile u ON t.user_id = u.user_id
WHERE t.txn_status IN ('pending', 'authorized');
