/* Staging model for Snowflake Warehouse Metering.
   This helps us track credit consumption across the account.
*/

with raw_metering as (
    select * from {{ source('snowflake_usage', 'warehouse_metering_history') }}
    -- We filter here monthly
    where start_time >= dateadd(day, -30, current_timestamp())
),

final as (
    select
        -- Surrogate Key (Unique ID for dbt tests)
        md5(cast(concat(warehouse_name, start_time) as string)) as metering_id,
        warehouse_name,
        start_time,
        end_time,
        
        -- Duration of the billing window in seconds
        datediff('second', start_time, end_time) as billed_seconds,
        
        -- All credit consumption
        credits_used,
        credits_used_compute,
        credits_used_cloud_services,
        
        -- Credit price (standard for example)
        4.0 as credit_price_usd
    from raw_metering
)

select * from final