select
    ticker,
    company_name,
    sector,
    industry,
    exchange
from {{ source('market_data', 'companies') }}