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
    COALESCE(r.txn_count_30d, 0) AS txn_count_30d,
    COALESCE(r.volume_30d, 0) AS volume_30d,
    COALESCE(r.avg_amount_30d, 0) AS avg_amount_30d,
    COALESCE(r.refund_rate_30d, 0) AS refund_rate_30d,
    COALESCE(r.decline_rate_30d, 0) AS decline_rate_30d,
    COALESCE(r.failure_rate_30d, 0) AS failure_rate_30d,
    COALESCE(r.unique_users_30d, 0) AS unique_users_30d,
    COALESCE(r.unique_devices_30d, 0) AS unique_devices_30d
FROM merchants m
LEFT JOIN risk r ON m.merchant_id = r.merchant_id
