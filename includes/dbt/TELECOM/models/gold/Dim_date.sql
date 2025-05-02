{{
    config(
<<<<<<< HEAD
        materialized='incremental',
        unique_key='Date_key',
        indexes=[{"columns": ['Date_key'], "unique": true}],
        target_schema='Gold'
    )
}}

with formatted_sms_date as (
    select 
        to_timestamp(timestamp, 'DD-MM-YYYY HH24:MI:SS') as formatted_timestamp
    from {{ source('row_data', 'SMS') }}
    where timestamp is not null
),
formatted_call_date as (
    select 
        to_timestamp(timestamp, 'DD-MM-YYYY HH24:MI:SS') as formatted_timestamp
    from {{ source('row_data', 'CALL_DATA') }}
    where timestamp is not null
),

unioned_dates as (
    select formatted_timestamp from formatted_sms_date
    union
    select formatted_timestamp from formatted_call_date
),

date_components as (
    select distinct
        formatted_timestamp as full_date,
        to_char(formatted_timestamp, 'YYYYMMDD')::int as Date_key,
        extract(day from formatted_timestamp) as day,
        extract(month from formatted_timestamp) as month,
        extract(year from formatted_timestamp) as year,
        to_char(formatted_timestamp, 'Day') as day_name,
        to_char(formatted_timestamp, 'Month') as month_name,
        extract(quarter from formatted_timestamp) as quarter,
        extract(dow from formatted_timestamp) as day_of_week,
        extract(doy from formatted_timestamp) as day_of_year,
        extract(hour from formatted_timestamp) as hour_24,
        to_char(formatted_timestamp, 'HH24:MI') as hour_minute,
        to_char(formatted_timestamp, 'HH12 AM') as hour_am_pm,
        concat('Q', extract(quarter from formatted_timestamp)) as quarter_name
    from unioned_dates
)

select * from date_components
{% if is_incremental() %}
where Date_key not in (select Date_key from {{ this }})
{% endif %}
=======
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
>>>>>>> 2872f63 (init reop)
