/*
    Incremental staging model for Snowflake Query History with cleaned columns and unit conversions
*/

{{
    config(
        materialized='incremental',
        unique_key='query_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ source('snowflake_usage', 'query_history') }}
    where warehouse_name is not null

    {% if is_incremental() %}
      and start_time >= dateadd(day, -3, (select max(start_time) from {{ this }}))
    {% else %}
      and start_time >= dateadd(day, -30, current_timestamp())
    {% endif %}
),

raw_query_history as (
    select
        query_id,
        query_text,
        database_name,
        schema_name,
        warehouse_name,
        warehouse_size,
        user_name,
        role_name,
        execution_status,
        query_tag,

        -- Timing and duration
        start_time,
        end_time,
        total_elapsed_time,
        datediff('second', start_time, end_time) as execution_time_seconds,

        -- Performance metrics
        rows_produced,
        bytes_scanned as total_bytes_scanned,
        percentage_scanned_from_cache,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        (bytes_scanned / power(1024, 3)) as gb_scanned,
        (bytes_spilled_to_local_storage / power(1024, 3)) as gb_spilled_local,
        (bytes_spilled_to_remote_storage / power(1024, 3)) as gb_spilled_remote,

        -- Data volume metrics
        rows_inserted,
        partitions_scanned,
        partitions_total,
        execution_time,
        error_code,
        error_message,

        -- More ms columns for deep analysis if needed
        compilation_time / 1000 as compilation_time_s,
        queued_overload_time / 1000 as queued_overload_time_s

    from source
)

select * from raw_query_history