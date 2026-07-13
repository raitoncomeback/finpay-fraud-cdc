WITH velocity_data AS (
    SELECT * FROM {{ source('risingwave', 'mv_user_velocity_7d') }}
)
SELECT
    user_id,
    txn_count_7d,
    amount_sum_7d,
    amount_avg_7d,
    amount_max_7d,
    unique_merchants_7d,
    unique_devices_7d,
    unique_countries_7d,
    unique_cities_7d,
    last_txn_at,
    first_txn_7d_at
FROM velocity_data
