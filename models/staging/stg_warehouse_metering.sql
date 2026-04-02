/* Staging model for Snowflake Warehouse Metering.
   This helps us track credit consumption across the account.
*/

with raw_metering as (
    select * from {{ source('snowflake_usage', 'warehouse_metering_history') }}
),

final as (
    select
        -- Using MD5 or Hash for a surrogate key is a dbt best practice
        md5(cast(concat(warehouse_name, start_time) as string)) as metering_id,
        warehouse_name,
        start_time,
        end_time,
        credits_used,
        credits_used_compute,
        credits_used_cloud_services,
        -- Random reference price (can be parameterized with a variable later)
        4.0 as credit_price_usd 
    from raw_metering
)

select * from final