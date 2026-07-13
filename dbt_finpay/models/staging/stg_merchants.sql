SELECT
    merchant_id,
    name,
    category,
    mcc_code,
    country,
    city,
    risk_level,
    is_active,
    onboarded_at,
    location_count
FROM {{ source('risingwave', 'silver_merchant_profile') }}
