{{
    config(
        materialized='incremental',
        unique_key='SITE_ID',
        indexes=[{"columns": ['SITE_ID'], "unique": true}],
        target_schema='Gold'
    )
}}

with cell_site_dim as (
    SELECT
        DISTINCT SITE_ID,
        CELL_ID,
        CITY,
        LATITUDE,
        LONGITUDE,
        SITE_NAME
    from {{ref("CDC_Cell_Site")}}
    where SITE_ID is not NULL
    and CELL_ID is not NULL
)

{% if is_incremental() %}
select * from cell_site_dim
where SITE_ID not in (select distinct SITE_ID from {{this}})

{% else %}

select * from cell_site_dim

{% endif %}