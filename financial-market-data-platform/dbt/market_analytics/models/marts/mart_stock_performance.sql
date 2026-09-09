with prices as (

    select *
    from {{ ref('stg_stock_prices') }}

),

companies as (

    select *
    from {{ ref('stg_companies') }}

),

price_metrics as (

    select
        ticker,
        trade_date,
        close_price,
        volume,

        lag(close_price) over (
            partition by ticker
            order by trade_date
        ) as previous_close,

        avg(close_price) over (
            partition by ticker
            order by trade_date
            rows between 4 preceding and current row
        ) as moving_avg_5d

    from prices

),

final as (

    select
        p.ticker,
        c.company_name,
        c.sector,
        c.industry,
        c.exchange,
        p.trade_date,
        p.close_price,
        p.volume,

        round(
            (
                (p.close_price - p.previous_close)
                / nullif(p.previous_close, 0)
            ) * 100,
            4
        ) as daily_return_pct,

        round(
            p.moving_avg_5d,
            4
        ) as moving_avg_5d

    from price_metrics p

    left join companies c
        on p.ticker = c.ticker

)

select *
from final