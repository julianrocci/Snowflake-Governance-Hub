/* Staging model for Snowflake Query History.
    Clean up names and convert units for better readability.
*/

with source as (
    select * from {{ source('snowflake_usage', 'query_history') }}
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
        
        -- Timing conversion (ms to seconds)
        execution_time / 1000 as execution_time_s,
        compilation_time / 1000 as compilation_time_s,
        queued_overload_time / 1000 as queued_overload_time_s,
        
        -- Data volume (bytes to GB)
        (bytes_scanned / power(1024, 3)) as gb_scanned,
        (bytes_spilled_to_local_storage / power(1024, 3)) as gb_spilled_local,
        (bytes_spilled_to_remote_storage / power(1024, 3)) as gb_spilled_remote,
        
        start_time,
        end_time
    from source
)

select * from raw_query_history