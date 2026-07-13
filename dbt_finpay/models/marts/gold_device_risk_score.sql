SELECT
    d.device_id,
    d.user_id,
    d.device_fingerprint,
    d.is_trusted,
    d.first_seen_at,
    d.last_seen_at,
    d.risk_score AS device_risk_score,
    CASE
        WHEN d.risk_score >= 80 THEN 'critical'
        WHEN d.risk_score >= 50 THEN 'high'
        WHEN d.risk_score >= 25 THEN 'medium'
        ELSE 'low'
    END AS risk_tier
FROM {{ ref('stg_devices') }} d
