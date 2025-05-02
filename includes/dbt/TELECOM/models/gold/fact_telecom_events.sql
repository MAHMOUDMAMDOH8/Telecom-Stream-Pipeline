{{ 
  config(
    materialized='incremental',
    unique_key='event_id',
    indexes=[{"columns": ['event_id'], "unique": true}],
    target_schema='Gold'
  ) 
}}

with sms_events as (
    select
        md5(sm.event_type || sm.timestamp || sm.imei_sender) as event_id,
        sm.event_type,
        sm.sender as sender_phone,
        substr(sm.imei_sender, 1, 8) as tac_sender,
        sm.sender_plan_type as plan_sender, 
        sm.sender_cell_site as cell_sender,
        sm.receiver as receiver_phone,
        substr(sm.imei_receiver, 1, 8) as tac_receiver,
        sm.receiver_plan_type as receiver_plan,
        sm.receiver_cell_site as cell_receiver,
        null::integer as call_duration_seconds,
        null::string as call_type,
        sm.status,
        try_to_timestamp(sm.timestamp, 'DD-MM-YYYY HH24:MI:SS') as event_timestamp,
        try_to_timestamp(sm.registration_date, 'DD-MM-YYYY') as reg_date,
        sm.amount,
        sm.currency
    from {{ source('row_data', 'SMS') }} as sm
),

call_events as (
    select 
        md5(ca.event_type || ca.timestamp || ca.device_caller) as event_id,
        ca.event_type,
        ca.caller as sender_phone,
        substr(ca.device_caller, 1, 8) as tac_sender,
        ca.caller_plan_type as plan_sender,
        ca.caller_cell_site as cell_sender,
        ca.receiver as receiver_phone,
        substr(ca.device_receiver, 1, 8) as tac_receiver,
        ca.receiver_plan_type as receiver_plan,
        ca.receiver_cell_site as cell_receiver,
        ca.call_duration_seconds,
        ca.call_type,
        ca.status,
        try_to_timestamp(ca.timestamp, 'DD-MM-YYYY HH24:MI:SS') as event_timestamp,
        null::timestamp as reg_date,
        ca.amount,
        ca.currency
    from {{ source('row_data', 'CALL_DATA') }} as ca
),

union_cte as (
    select * from sms_events
    union all
    select * from call_events
),

final_cte as (
    select 
        ev.event_id,
        sender.user_id as sender_id,
        receiver.user_id as receiver_id,
        sender_site.site_id as sender_site_id,
        receiver_site.site_id as receiver_site_id,
        send_tac.tac_id as sender_device_id,
        rec_tac.tac_id as receiver_device_id,
        date_k.date_key as date_key,
        ev.plan_sender,
        ev.receiver_plan,
        ev.call_duration_seconds,
        ev.call_type,
        ev.status,
        ev.reg_date,
        ev.amount,
        ev.currency
    from union_cte as ev
    left join {{ ref("Dim_user") }} as sender
        on ev.sender_phone = sender.phone_number
    left join {{ ref("Dim_user") }} as receiver
        on ev.receiver_phone = receiver.phone_number
    left join {{ ref("Dim_cell_site") }} as sender_site
        on ev.cell_sender = sender_site.cell_id
    left join {{ ref("Dim_cell_site") }} as receiver_site
        on ev.cell_receiver = receiver_site.cell_id
    left join {{ ref("Dim_device_tac") }} as send_tac 
        on ev.tac_sender = send_tac.tac_code
    left join {{ ref("Dim_device_tac") }} as rec_tac
        on ev.tac_receiver = rec_tac.tac_code
    left join {{ ref("Dim_date") }} as date_k
        on ev.event_timestamp = date_k.full_date
)

{% if is_incremental() %}
select * from final_cte
where event_id not in (select distinct event_id from {{ this }})

{% else %}

select * from final_cte

{% endif %}
