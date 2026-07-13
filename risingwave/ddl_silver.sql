-- RisingWave DDL: Silver Layer (Enriched, Deduplicated)
-- Materialized views that join bronze sources for enriched views

-- ============================================================
-- SILVER: Transactions Enriched with User + Merchant + Location + Device
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS silver_transactions_enriched AS
SELECT
    t.transaction_id,
    t.account_id,
    t.user_id,
    t.merchant_id,
    t.location_id,
    t.device_id,

    -- Transaction core
    t.amount,
    t.currency,
    t.txn_type,
    t.txn_status,
    t.entry_mode,
    t.card_present,
    t.is_3ds,
    t.is_tokenized,
    t.description,
    t.metadata,
    t.initiated_at,
    t.authorized_at,
    t.settled_at,

    -- User enrichment
    u.email AS user_email,
    u.kyc_status AS user_kyc_status,
    u.risk_score AS user_risk_score,
    u.created_at AS user_created_at,

    -- Account enrichment
    a.account_type AS user_account_type,
    a.balance AS user_account_balance,

    -- Merchant enrichment
    m.name AS merchant_name,
    m.category AS merchant_category,
    m.mcc_code AS merchant_mcc_code,
    m.country AS merchant_country,
    m.city AS merchant_city,
    m.risk_level AS merchant_risk_level,

    -- Location enrichment
    ml.country AS txn_country,
    ml.city AS txn_city,
    ml.lat AS txn_lat,
    ml.lon AS txn_lon,
    ml.is_online,

    -- Device enrichment
    d.device_fingerprint,
    d.device_type,
    d.os AS device_os,
    d.is_trusted AS device_is_trusted,
    d.first_seen_at AS device_first_seen_at,

    -- Derived fields
    t.initiated_at::DATE AS txn_date,
    EXTRACT(HOUR FROM t.initiated_at) AS txn_hour,
    CASE WHEN t.amount > 5000 THEN TRUE ELSE FALSE END AS is_high_amount,
    CASE WHEN NOT t.card_present THEN TRUE ELSE FALSE END AS is_card_not_present,
    CASE WHEN NOT d.is_trusted THEN TRUE ELSE FALSE END AS is_new_device

FROM finpay_transactions_cdc t
LEFT JOIN finpay_users_cdc u ON t.user_id = u.user_id
LEFT JOIN finpay_accounts_cdc a ON t.account_id = a.account_id
LEFT JOIN finpay_merchants_cdc m ON t.merchant_id = m.merchant_id
LEFT JOIN finpay_merchant_locations_cdc ml ON t.location_id = ml.location_id
LEFT JOIN finpay_devices_cdc d ON t.device_id = d.device_id;


-- ============================================================
-- SILVER: User Profile (Latest State)
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS silver_user_profile AS
SELECT
    u.user_id,
    u.email,
    u.phone,
    u.full_name,
    u.kyc_status,
    u.risk_score,
    CASE
        WHEN u.risk_score >= 80 THEN 'critical'
        WHEN u.risk_score >= 60 THEN 'elevated'
        ELSE 'standard'
    END AS risk_tier,
    u.created_at,
    u.updated_at,
    COUNT(DISTINCT a.account_id) AS account_count,
    COALESCE(SUM(a.balance), 0) AS total_balance
FROM finpay_users_cdc u
LEFT JOIN finpay_accounts_cdc a ON u.user_id = a.user_id
GROUP BY u.user_id, u.email, u.phone, u.full_name, u.kyc_status,
         u.risk_score, u.created_at, u.updated_at;


-- ============================================================
-- SILVER: Merchant Profile
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS silver_merchant_profile AS
SELECT
    m.merchant_id,
    m.name,
    m.category,
    m.mcc_code,
    m.country,
    m.city,
    m.risk_level,
    m.is_active,
    m.onboarded_at,
    COUNT(DISTINCT ml.location_id) AS location_count
FROM finpay_merchants_cdc m
LEFT JOIN finpay_merchant_locations_cdc ml ON m.merchant_id = ml.merchant_id
GROUP BY m.merchant_id, m.name, m.category, m.mcc_code, m.country, m.city,
         m.risk_level, m.is_active, m.onboarded_at;


-- ============================================================
-- SILVER: Device Reputation
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS silver_device_reputation AS
SELECT
    d.device_id,
    d.user_id,
    d.device_fingerprint,
    d.device_type,
    d.os,
    d.browser,
    d.is_trusted,
    d.first_seen_at,
    d.last_seen_at,
    CASE
        WHEN NOT d.is_trusted AND d.first_seen_at >= NOW() - INTERVAL '7 days' THEN 50.0
        WHEN NOT d.is_trusted THEN 25.0
        ELSE 10.0
    END AS risk_score
FROM finpay_devices_cdc d
WHERE d.first_seen_at IS NOT NULL;
