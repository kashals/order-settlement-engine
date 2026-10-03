CREATE TABLE processed_events (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    amount NUMERIC(12, 2) NOT NULL,
    payment_status VARCHAR(32) NOT NULL,
    transaction_reference VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
