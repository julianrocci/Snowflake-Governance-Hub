/*
    Fact Table: Data Skew Analysis
    Materialized as a table for BI performance. 
    This model filters out balanced queries to focus only on performance bottlenecks.
*/

{{ config(
    materialized='table'
) }}

SELECT
    query_id,
    user_name,
    warehouse_name,
    warehouse_size,
    total_exec_seconds,
    partitions_scanned,
    seconds_per_partition,
    skew_status,
    query_text,
    -- Audit column to know when the data was last refreshed
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_data_skew') }}
-- We only keep queries that require attention
WHERE skew_status IN ('POTENTIAL_SKEW', 'CRITICAL_SKEW')
ORDER BY total_exec_seconds DESC