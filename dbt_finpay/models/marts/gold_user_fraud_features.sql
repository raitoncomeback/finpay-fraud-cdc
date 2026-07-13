WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),
velocity AS (
    SELECT * FROM {{ source('risingwave', 'mv_user_velocity_7d') }}
)
SELECT
    u.user_id,
    u.email,
    u.kyc_status,
    u.risk_score AS user_risk_score,
    u.risk_tier,
    u.account_count,
    u.total_balance,
    COALESCE(v.txn_count_7d, 0) AS txn_count_7d,
    COALESCE(v.amount_sum_7d, 0) AS amount_sum_7d,
    COALESCE(v.amount_avg_7d, 0) AS amount_avg_7d,
    COALESCE(v.unique_merchants_7d, 0) AS unique_merchants_7d,
    COALESCE(v.unique_devices_7d, 0) AS unique_devices_7d,
    COALESCE(v.unique_countries_7d, 0) AS unique_countries_7d
FROM users u
LEFT JOIN velocity v ON u.user_id = v.user_id
