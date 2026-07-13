-- FinPay OLTP Schema (PostgreSQL)
-- Synthetic financial transaction database for CDC demo

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- USERS & ACCOUNTS
-- ============================================================

CREATE TABLE users (
    user_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email             VARCHAR(255) UNIQUE NOT NULL,
    phone             VARCHAR(20),
    full_name         VARCHAR(255) NOT NULL,
    kyc_status        VARCHAR(20) DEFAULT 'pending' CHECK (kyc_status IN ('pending', 'verified', 'rejected')),
    risk_score        INTEGER DEFAULT 0 CHECK (risk_score >= 0 AND risk_score <= 100),
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE accounts (
    account_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id           UUID NOT NULL REFERENCES users(user_id),
    account_type      VARCHAR(20) NOT NULL CHECK (account_type IN ('checking', 'savings', 'credit')),
    currency          VARCHAR(3) DEFAULT 'USD',
    balance           DECIMAL(18,2) DEFAULT 0.00,
    status            VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed')),
    opened_at         TIMESTAMPTZ DEFAULT NOW(),
    closed_at         TIMESTAMPTZ
);

-- ============================================================
-- MERCHANTS
-- ============================================================

CREATE TABLE merchants (
    merchant_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name              VARCHAR(255) NOT NULL,
    category          VARCHAR(50) NOT NULL,
    mcc_code          VARCHAR(4),
    country           VARCHAR(2) NOT NULL,
    city              VARCHAR(100),
    risk_level        VARCHAR(20) DEFAULT 'medium' CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    onboarded_at      TIMESTAMPTZ DEFAULT NOW(),
    is_active         BOOLEAN DEFAULT TRUE
);

CREATE TABLE merchant_locations (
    location_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    merchant_id       UUID NOT NULL REFERENCES merchants(merchant_id),
    terminal_id       VARCHAR(50) UNIQUE NOT NULL,
    country           VARCHAR(2) NOT NULL,
    city              VARCHAR(100),
    lat               DECIMAL(10, 8),
    lon               DECIMAL(11, 8),
    is_online         BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- DEVICES (for device fingerprinting)
-- ============================================================

CREATE TABLE devices (
    device_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id           UUID NOT NULL REFERENCES users(user_id),
    device_fingerprint VARCHAR(255) UNIQUE NOT NULL,
    device_type       VARCHAR(20) CHECK (device_type IN ('mobile', 'desktop', 'tablet', 'pos', 'atm')),
    os                VARCHAR(50),
    browser           VARCHAR(50),
    ip_address        INET,
    country           VARCHAR(2),
    city              VARCHAR(100),
    lat               DECIMAL(10, 8),
    lon               DECIMAL(11, 8),
    is_trusted        BOOLEAN DEFAULT FALSE,
    first_seen_at     TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TRANSACTIONS (Core CDC table)
-- ============================================================

CREATE TABLE transactions (
    transaction_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id        UUID NOT NULL REFERENCES accounts(account_id),
    user_id           UUID NOT NULL REFERENCES users(user_id),
    merchant_id       UUID REFERENCES merchants(merchant_id),
    location_id       UUID REFERENCES merchant_locations(location_id),
    device_id         UUID REFERENCES devices(device_id),

    -- Transaction details
    amount            DECIMAL(18,2) NOT NULL CHECK (amount > 0),
    currency          VARCHAR(3) DEFAULT 'USD',
    txn_type          VARCHAR(20) NOT NULL CHECK (txn_type IN ('purchase', 'withdrawal', 'deposit', 'transfer', 'refund', 'fee')),
    txn_status        VARCHAR(20) DEFAULT 'pending' CHECK (txn_status IN ('pending', 'authorized', 'settled', 'declined', 'reversed', 'failed')),

    -- Fraud-relevant fields
    entry_mode        VARCHAR(20) CHECK (entry_mode IN ('chip', 'swipe', 'contactless', 'online', 'keyed', 'recurring')),
    card_present      BOOLEAN,
    is_3ds            BOOLEAN DEFAULT FALSE,
    is_tokenized      BOOLEAN DEFAULT FALSE,

    -- Metadata
    description       VARCHAR(500),
    metadata          JSONB,

    -- Timestamps
    initiated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    authorized_at     TIMESTAMPTZ,
    settled_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- FRAUD SIGNALS (Appended by fraud engine)
-- ============================================================

CREATE TABLE fraud_signals (
    signal_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id    UUID NOT NULL REFERENCES transactions(transaction_id),
    signal_type       VARCHAR(50) NOT NULL,
    signal_value      DECIMAL(10,4),
    threshold         DECIMAL(10,4),
    is_anomaly        BOOLEAN DEFAULT FALSE,
    model_version     VARCHAR(20),
    detected_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INDEXES for OLTP performance
-- ============================================================

CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_merchant_id ON transactions(merchant_id);
CREATE INDEX idx_transactions_device_id ON transactions(device_id);
CREATE INDEX idx_transactions_initiated_at ON transactions(initiated_at DESC);
CREATE INDEX idx_transactions_status ON transactions(txn_status);
CREATE INDEX idx_transactions_amount ON transactions(amount);
CREATE INDEX idx_fraud_signals_txn_id ON fraud_signals(transaction_id);

-- ============================================================
-- DEBEZIUM: Enable REPLICA IDENTITY for all tables
-- ============================================================

ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE merchant_locations REPLICA IDENTITY FULL;
ALTER TABLE devices REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE fraud_signals REPLICA IDENTITY FULL;

-- ============================================================
-- PUBLICATION for Debezium
-- ============================================================

CREATE PUBLICATION finpay_cdc FOR TABLE
    users,
    accounts,
    merchants,
    merchant_locations,
    devices,
    transactions,
    fraud_signals;