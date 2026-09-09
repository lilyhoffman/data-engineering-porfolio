CREATE TABLE IF NOT EXISTS stock_prices (
    ticker VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    adjusted_close NUMERIC(12, 4),
    volume BIGINT,
    ingestion_timestamp TIMESTAMP NOT NULL,

    PRIMARY KEY (ticker, trade_date)
);


CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    dag_run_id VARCHAR(250),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    rows_inserted INTEGER DEFAULT 0,
    error_message TEXT
);


CREATE TABLE IF NOT EXISTS companies (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(150),
    exchange VARCHAR(20)
);