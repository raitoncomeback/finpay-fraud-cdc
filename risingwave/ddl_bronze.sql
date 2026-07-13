-- RisingWave DDL: Bronze Layer (Raw CDC Events)
-- Source: Kafka topics from Debezium
-- RisingWave 3.x: FORMAT PLAIN ENCODE JSON, no DECIMAL precision in sources

-- ============================================================
-- CREATE KAFKA SOURCES
-- ============================================================

-- Users CDC
CREATE SOURCE IF NOT EXISTS finpay_users_cdc (
    user_id VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    full_name VARCHAR,
    kyc_status VARCHAR,
    risk_score INT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.users',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Accounts CDC
CREATE SOURCE IF NOT EXISTS finpay_accounts_cdc (
    account_id VARCHAR,
    user_id VARCHAR,
    account_type VARCHAR,
    currency VARCHAR,
    balance DOUBLE,
    status VARCHAR,
    opened_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.accounts',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Merchants CDC
CREATE SOURCE IF NOT EXISTS finpay_merchants_cdc (
    merchant_id VARCHAR,
    name VARCHAR,
    category VARCHAR,
    mcc_code VARCHAR,
    country VARCHAR,
    city VARCHAR,
    risk_level VARCHAR,
    onboarded_at TIMESTAMPTZ,
    is_active BOOLEAN
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.merchants',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Merchant Locations CDC
CREATE SOURCE IF NOT EXISTS finpay_merchant_locations_cdc (
    location_id VARCHAR,
    merchant_id VARCHAR,
    terminal_id VARCHAR,
    country VARCHAR,
    city VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    is_online BOOLEAN
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.merchant_locations',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Devices CDC
CREATE SOURCE IF NOT EXISTS finpay_devices_cdc (
    device_id VARCHAR,
    user_id VARCHAR,
    device_fingerprint VARCHAR,
    device_type VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    ip_address VARCHAR,
    country VARCHAR,
    city VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    is_trusted BOOLEAN,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.devices',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Transactions CDC (high volume)
CREATE SOURCE IF NOT EXISTS finpay_transactions_cdc (
    transaction_id VARCHAR,
    account_id VARCHAR,
    user_id VARCHAR,
    merchant_id VARCHAR,
    location_id VARCHAR,
    device_id VARCHAR,
    amount DOUBLE,
    currency VARCHAR,
    txn_type VARCHAR,
    txn_status VARCHAR,
    entry_mode VARCHAR,
    card_present BOOLEAN,
    is_3ds BOOLEAN,
    is_tokenized BOOLEAN,
    description VARCHAR,
    metadata VARCHAR,
    initiated_at TIMESTAMPTZ,
    authorized_at TIMESTAMPTZ,
    settled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'finpay.public.transactions',
    properties.bootstrap.server = 'kafka:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
