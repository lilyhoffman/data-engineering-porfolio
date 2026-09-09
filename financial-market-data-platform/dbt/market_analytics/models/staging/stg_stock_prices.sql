select
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close,
    volume,
    ingestion_timestamp
from {{ source('market_data', 'stock_prices') }}