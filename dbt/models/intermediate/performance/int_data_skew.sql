/*
    Intermediate Model: Data Skew Detection
    Focuses on execution efficiency per partition scanned.
    Identifies queries where a few workers are overloaded compared to the average.
*/

{{ config(
    materialized='view'
) }}

WITH compute_metrics AS (
    -- Gather base metrics for the last month
    SELECT
        query_id,
        query_text,
        user_name,
        warehouse_name,
        warehouse_size,
        execution_time / 1000 AS total_exec_seconds,
        partitions_scanned,
        start_time
    FROM {{ ref('stg_query_history') }}
    WHERE start_time >= DATEADD('month', -1, CURRENT_TIMESTAMP())
      AND execution_time > 0
      AND partitions_scanned > 50 -- Ignore very small metadata queries
),

skew_calculations AS (
    -- Calculate processing time per partition
    SELECT
        *,
        (total_exec_seconds / NULLIF(partitions_scanned, 0)) AS seconds_per_partition
    FROM compute_metrics
)

SELECT
    query_id,
    user_name,
    warehouse_name,
    warehouse_size,
    total_exec_seconds,
    partitions_scanned,
    seconds_per_partition,
    CASE
        -- CRITICAL: Bottleneck confirmed (>0.5s/partition) and impact is high (>5min)
        WHEN seconds_per_partition > 0.5 AND total_exec_seconds > 300 THEN 'CRITICAL_SKEW'
        
        -- POTENTIAL: Bottleneck confirmed (>0.5s/partition) but impact is moderate (>1min)
        WHEN seconds_per_partition > 0.5 AND total_exec_seconds > 60 THEN 'POTENTIAL_SKEW'
        
        ELSE 'BALANCED'
    END AS skew_status,
    query_text
FROM skew_calculations