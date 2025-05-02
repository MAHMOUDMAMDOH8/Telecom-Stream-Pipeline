{{
    config(
        target_schema='gold'
    )
}}

with Dim_companies as (
    select
        *
    from {{ source('row_data', 'companies') }}
)

select * from Dim_companies
