{{
    config(
        materialized='incremental',
        unique_key='TAC_ID',
        indexes=[{"columns": ['TAC_ID'], "unique": true}],
        target_schema='Gold'
    )
}}

with TAC_data as (
    select
        md5(TAC_CODE) as TAC_ID,
        MANUFACTURER,
        TAC_CODE
    from {{ source('row_data', 'device_tac_lookup') }}
) 

{% if is_incremental() %}
select * from TAC_data
where TAC_ID not in (select distinct TAC_ID from {{this}})

{% else %}

select * from TAC_data

{% endif %}