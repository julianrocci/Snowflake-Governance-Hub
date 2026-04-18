/* Intermediate: Data Spilling Metrics.
   Analyzes memory overflow (spilling) based on 'logicals' thresholds.
   Includes query identity for Top 10 deep-dives.
*/

{{ config(
    materialized='ephemeral'
) }}

WITH base_metrics AS (
    SELECT
        query_id,
        query_text,         -- Usefull for Top 10 worst queries analysis
        user_name,          -- Usefull to identify who ran the query
        warehouse_name,
        start_time,
        total_elapsed_time,
        percentage_scanned_from_cache,
        bytes_spilled_to_local_storage AS bytes_local,
        bytes_spilled_to_remote_storage AS bytes_remote,
        total_bytes_scanned AS total_bytes
    FROM {{ ref('stg_query_history') }}
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND warehouse_size IS NOT NULL
)

SELECT
    *,
    -- Flag for Local Spilling > 30% ( bad queries )
    CASE 
        WHEN total_bytes > 0 AND (bytes_local / total_bytes) > 0.30 
        THEN 1 ELSE 0
    END AS is_bad_local_spill,

    -- Flag for Remote Spilling > 1% ( critical queries )
    CASE 
        WHEN total_bytes > 0 AND (bytes_remote / total_bytes) > 0.01 
        THEN 1 ELSE 0
    END AS is_critical_remote_spill
FROM base_metrics