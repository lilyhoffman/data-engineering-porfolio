SELECT
    ticker,
    trade_date,
    close_price,
    daily_return_pct,
    moving_avg_5d

FROM {{ ref('mart_stock_performance') }}

WHERE
    close_price <= 0
    OR moving_avg_5d <= 0