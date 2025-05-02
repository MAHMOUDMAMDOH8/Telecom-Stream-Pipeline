{% snapshot CDC_Cell_Site %}

{{
    config(
        target_schema='snapshots',
        unique_key='SITE_ID',
        strategy='check',
        check_cols=['CITY', 'LATITUDE','LONGITUDE','SITE_NAME']
    )
}}

select 
    md5(CELL_ID) as SITE_ID,
    CELL_ID,
    CITY,
    LATITUDE,
    LONGITUDE,
    SITE_NAME,
    current_timestamp() as snapshot_timestamp
from {{ source('row_data', 'CELL_SITE') }}
{% endsnapshot %}