SELECT
    user_id,
    email,
    phone,
    full_name,
    kyc_status,
    risk_score,
    risk_tier,
    created_at,
    updated_at,
    account_count,
    total_balance
FROM {{ source('risingwave', 'silver_user_profile') }}
