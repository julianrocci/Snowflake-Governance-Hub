/* Staging model for Snowflake Query History.
    Clean up names and convert units for better readability.
*/

with source as (
    select * from {{ source('snowflake_usage', 'query_history') }}
    where warehouse_name is not null    -- Serverless excluded
      and start_time >= dateadd(day, -30, current_timestamp())
),

raw_query_history as (
    select
        query_id,
        query_text,
        database_name,
        schema_name,
        warehouse_name,
        user_name,
        role_name,
        execution_status,
        query_tag,
        
        -- Timing and duration
        start_time,
        end_time,
        datediff('second', start_time, end_time) as execution_time_seconds,
        
        -- Performance metrics
        rows_produced,
        (bytes_scanned / power(1024, 3)) as gb_scanned,
        (bytes_spilled_to_local_storage / power(1024, 3)) as gb_spilled_local,
        (bytes_spilled_to_remote_storage / power(1024, 3)) as gb_spilled_remote,
        
        -- More ms columns for deep analysis if needed
        compilation_time / 1000 as compilation_time_s,
        queued_overload_time / 1000 as queued_overload_time_s
        
    from source
)
select * from raw_query_history