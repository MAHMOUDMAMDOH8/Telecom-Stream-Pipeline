{{
    config(
        target_schema='gold'
    )
}}

with formatted_stock_prices as (
    select 
        to_timestamp(timestamp, 'DD-MM-YYYY HH24:MI') as formatted_timestamp
    from {{ source('row_data', 'stock_prices') }}
    where timestamp is not null
),

Dim_date as (
    select 
        distinct
        formatted_timestamp as date,
        md5(formatted_timestamp::varchar) as date_key,
        date_part('year', formatted_timestamp) as year, 
        date_part('month', formatted_timestamp) as month,
        date_part('day', formatted_timestamp) as day,
        date_part('dayofweek', formatted_timestamp) as day_of_week,
        date_part('quarter', formatted_timestamp) as quarter,
        date_part('week', formatted_timestamp) as week
    from formatted_stock_prices
)
select * from Dim_date
