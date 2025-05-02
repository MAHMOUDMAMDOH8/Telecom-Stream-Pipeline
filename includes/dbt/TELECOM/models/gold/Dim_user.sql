{{
    config(
        materialized='incremental',
        unique_key='USER_ID',
        indexes=[{"columns": ['USER_ID'], "unique": true}],
        target_schema='Gold'
    )
}}

with users as (
    select
        distinct USER_ID,
        FIRST_NAME,
        LAST_NAME,
        PHONE_NUMBER,
        CITY,
        EMAIL,
        SEX
    from {{ref('CDC_Users')}}
    where 
        USER_ID is not null
    and FIRST_NAME is not null 
    and LAST_NAME is not null 
    and PHONE_NUMBER is not null
    and CITY is not null
)


{% if is_incremental() %}
select * from users
where USER_ID not in (select distinct USER_ID from {{this}})

{% else %}

select * from users

{% endif %}