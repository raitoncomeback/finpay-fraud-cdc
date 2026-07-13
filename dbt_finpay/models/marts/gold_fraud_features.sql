WITH risk_data AS (
    SELECT * FROM {{ source('risingwave', 'mv_transaction_risk_score') }}
)
SELECT
    transaction_id,
    user_id,
    merchant_id,
    device_id,
    amount,
    txn_status,
    initiated_at,
    composite_risk_score,
    risk_tier,
    device_risk_score,
    user_risk_score,
    txn_count_7d,
    amount_sum_7d,
    amount_avg_7d,
    unique_devices_7d,
    txn_count_30d,
    amount_sum_30d,
    refund_rate_30d,
    decline_rate_30d,
    user_kyc_status
FROM risk_data
