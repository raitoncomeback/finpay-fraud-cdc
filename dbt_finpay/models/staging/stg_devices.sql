SELECT
    device_id,
    user_id,
    device_fingerprint,
    device_type,
    os,
    browser,
    is_trusted,
    first_seen_at,
    last_seen_at,
    risk_score
FROM {{ source('risingwave', 'silver_device_reputation') }}
