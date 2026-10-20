CREATE TABLE IF NOT EXISTS public.streamguard_event_ledger (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Stable identity of the Kafka record
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),

    -- Event and transaction details
    message_key TEXT,
    operation TEXT,
    transaction_id UUID,
    customer_id BIGINT,
    merchant_id BIGINT,
    amount NUMERIC(12, 2),
    currency TEXT,

    -- Processing result
    is_suspicious BOOLEAN NOT NULL DEFAULT FALSE,
    risk_reason TEXT,

    -- Preserve the complete Debezium envelope
    event_payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Prevent the same Kafka record being registered twice
    CONSTRAINT uq_streamguard_event
        UNIQUE (topic, partition_id, kafka_offset)
);

CREATE TABLE IF NOT EXISTS public.streamguard_fraud_alerts (
    alert_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- One alert at most per source event
    event_id BIGINT NOT NULL UNIQUE
        REFERENCES public.streamguard_event_ledger(event_id),

    transaction_id UUID NOT NULL,
    customer_id BIGINT NOT NULL,
    merchant_id BIGINT NOT NULL,

    amount NUMERIC(12, 2) NOT NULL,
    currency TEXT NOT NULL CHECK (currency = 'INR'),
    threshold_amount NUMERIC(12, 2) NOT NULL,
    risk_reason TEXT NOT NULL,

    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_alert_exceeds_threshold
        CHECK (amount > threshold_amount)
);

CREATE INDEX IF NOT EXISTS idx_streamguard_alerts_transaction
    ON public.streamguard_fraud_alerts (transaction_id, detected_at DESC);
