
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    home_region TEXT NOT NULL,
    account_status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    merchant_name TEXT NOT NULL UNIQUE,
    merchant_category TEXT NOT NULL,
    merchant_region TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    merchant_id BIGINT NOT NULL REFERENCES merchants(merchant_id),
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
    ON transactions(event_time);

CREATE INDEX IF NOT EXISTS idx_transactions_customer_id
    ON transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id
    ON transactions(merchant_id);
