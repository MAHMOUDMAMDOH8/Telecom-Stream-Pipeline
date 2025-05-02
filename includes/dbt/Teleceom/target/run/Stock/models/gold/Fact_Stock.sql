
  create or replace   view STOCK.public_silver.Fact_Stock
  
   as (
    

with formatted_stock_prices as (
    select
        *,
        to_timestamp(timestamp, 'DD-MM-YYYY HH24:MI') as formatted_timestamp
    from STOCK.SILVER.stock_prices
),

Fact_Stock as (
    select 
        row_number() over (partition by formatted_timestamp, companies.symbol order by formatted_timestamp) as id,
        companies.symbol as company_id,
        date.date_key as date_key,
        stock_prices.open as open_price,
        stock_prices.close as close_price,
        stock_prices.high as high_price,
        stock_prices.low as low_price,
        stock_prices.volume as volume
    from formatted_stock_prices as stock_prices
    join STOCK.public_silver.Dim_companies as companies
        on stock_prices.symbol = companies.symbol
    join STOCK.public_silver.Dim_date as date
        on stock_prices.formatted_timestamp = date.date
)

select *
from Fact_Stock
  );

