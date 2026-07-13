WITH merchants AS (
    SELECT * FROM {{ ref('stg_merchants') }}
),
risk AS (
    SELECT * FROM {{ source('risingwave', 'mv_merchant_risk_realtime') }}
)
SELECT
    m.merchant_id,
    m.name,
    m.category,
    m.mcc_code,
    m.risk_level AS merchant_risk_level,
    m.is_active,
    m.location_count,
    COALESCE(r.txn_count_1h, 0) AS txn_count_1h,
    COALESCE(r.volume_1h, 0) AS volume_1h,
    COALESCE(r.avg_amount_1h, 0) AS avg_amount_1h,
    COALESCE(r.refund_rate_1h, 0) AS refund_rate_1h,
    COALESCE(r.decline_rate_1h, 0) AS decline_rate_1h,
    COALESCE(r.failure_rate_1h, 0) AS failure_rate_1h,
    COALESCE(r.unique_users_1h, 0) AS unique_users_1h,
    COALESCE(r.unique_devices_1h, 0) AS unique_devices_1h
FROM merchants m
LEFT JOIN risk r ON m.merchant_id = r.merchant_id
