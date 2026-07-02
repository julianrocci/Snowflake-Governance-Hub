-- Staging model for external CRM customer events with pre_hook refresh from S3

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        pre_hook=[
            "ALTER EXTERNAL TABLE {{ source('crm_events', 'ext_customer_events').render() }} REFRESH"
        ]
    )
}}

with source as (
    select
        value:event_id::string as event_id,
        value:customer_id::int as customer_id,
        value:event_type::string as event_type,
        value:event_timestamp::timestamp_ntz as event_timestamp,
        value:metadata:channel::string as channel,
        value:metadata:amount::decimal(10,2) as amount
    from {{ source('crm_events', 'ext_customer_events') }}

    {% if is_incremental() %}
      where event_timestamp >= dateadd(hour, -6, (select max(event_timestamp) from {{ this }}))
    {% endif %}
)

select * from source