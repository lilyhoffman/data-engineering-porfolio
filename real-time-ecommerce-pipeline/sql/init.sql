CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    category VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    order_total NUMERIC(12, 2) NOT NULL,
    order_timestamp TIMESTAMPTZ NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL,
    processed_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    category VARCHAR(50) NOT NULL,
    order_count BIGINT NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    avg_order_value NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (window_start, window_end, category)
);