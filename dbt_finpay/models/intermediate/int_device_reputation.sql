WITH device_data AS (
    SELECT * FROM {{ source('risingwave', 'mv_device_risk_realtime') }}
)
SELECT
    device_id,
    user_id,
    device_fingerprint,
    is_trusted,
    first_seen_at,
    last_seen_at,
    risk_score
FROM device_data
