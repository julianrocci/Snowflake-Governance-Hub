/* Mart: Query Spilling Details.
   Provides granular details for queries exceeding spilling thresholds.
   Used for deep-dive analysis and Top 10 problem queries all domains combined.
*/

{{ config(
    materialized='table'
) }}

WITH int_spilling AS (
    SELECT * FROM {{ ref('int_warehouse_spilling_performance') }}
)

SELECT
    query_id,
    query_text,
    user_name,
    warehouse_name,
    start_time,
    total_elapsed_time AS execution_time_ms,
    
    -- Volumes for sorting and impact analysis (converted to MB for more precision)
    ROUND(total_bytes / POWER(1024, 2), 2) AS total_mb_scanned,
    ROUND(bytes_local / POWER(1024, 2), 2) AS spilled_local_mb,
    ROUND(bytes_remote / POWER(1024, 2), 2) AS spilled_remote_mb,
    
    -- Threshold flags from intermediate
    is_bad_local_spill,
    is_critical_remote_spill,

    {{ get_domain_from_warehouse('warehouse_name') }} AS domain

FROM int_spilling
-- We only keep the "bad" queries for this mart
WHERE is_bad_local_spill = 1 OR is_critical_remote_spill = 1