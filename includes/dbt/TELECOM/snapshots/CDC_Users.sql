{% snapshot CDC_Users %}

{{
    config(
        target_schema='snapshots',
        unique_key='USER_ID',
        strategy='check',
        check_cols=['CITY', 'EMAIL']
    )
}}

select 
    md5(PHONE_NUMBER) as USER_ID,
    FIRST_NAME,
    LAST_NAME,
    PHONE_NUMBER,
    CITY,
    EMAIL,
    SEX,
    current_timestamp() as snapshot_timestamp
from {{ source('row_data', 'USERS') }}


{% endsnapshot %}