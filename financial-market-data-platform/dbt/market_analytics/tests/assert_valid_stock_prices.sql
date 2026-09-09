SELECT
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume

FROM {{ ref('stg_stock_prices') }}

WHERE
    open_price <= 0
    OR close_price <= 0
    OR high_price < low_price
    OR volume < 0