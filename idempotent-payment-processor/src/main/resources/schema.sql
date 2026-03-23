DROP TABLE IF EXISTS processed_messages;
DROP TABLE IF EXISTS payment_outbox;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS target_accounts;

CREATE TABLE payments (
    id UUID PRIMARY KEY,
    amount NUMERIC(19, 4) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE payment_outbox (
    id SERIAL PRIMARY KEY,
    message_id UUID NOT NULL UNIQUE,
    payment_id UUID NOT NULL REFERENCES payments(id),
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Deduplication table for the consumer side
CREATE TABLE processed_messages (
    message_id UUID PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- State modified by the consumer
CREATE TABLE target_accounts (
    id UUID PRIMARY KEY,
    balance NUMERIC(19, 4) NOT NULL DEFAULT 0.00
);
