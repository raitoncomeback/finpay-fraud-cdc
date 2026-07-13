WITH merchant_data AS (
    SELECT * FROM {{ source('risingwave', 'mv_merchant_risk_realtime') }}
)
SELECT
    merchant_id,
    txn_count_1h,
    volume_1h,
    avg_amount_1h,
    refund_rate_1h,
    decline_rate_1h,
    failure_rate_1h,
    unique_users_1h,
    unique_devices_1h
FROM merchant_data
