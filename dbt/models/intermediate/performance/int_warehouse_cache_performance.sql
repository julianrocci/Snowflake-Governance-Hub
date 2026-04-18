/* Intermediate model to flag query performance based on cache usage.
   Logic:
   - Result Cache: 100% match.
   - Local Disk: > 50% of total bytes scanned.
   - Remote Disk: > 50% of total bytes scanned.
*/
{{ config(
    materialized='ephemeral'
) }}

WITH base_metrics AS (
    SELECT
        query_id,
        warehouse_name,
        start_time,
        total_elapsed_time,
        percentage_scanned_from_cache,
        total_bytes_scanned AS total_bytes,
        (total_bytes_scanned * percentage_scanned_from_cache) AS bytes_from_cache,
        (total_bytes_scanned * (1 - percentage_scanned_from_cache)) AS bytes_from_remote
    FROM {{ ref('stg_query_history') }}
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND warehouse_size IS NOT NULL
),

flagged_queries AS (
    SELECT
        *,
        CASE 
            WHEN percentage_scanned_from_cache = 1 THEN 1
            ELSE 0
        END AS is_result_cache_hit,

        CASE
            WHEN percentage_scanned_from_cache > 0.50 THEN 1
            ELSE 0
        END AS is_local_efficient,

        CASE
            WHEN percentage_scanned_from_cache < 0.10 AND total_bytes > 0 THEN 1
            ELSE 0
        END AS is_remote_heavy
    FROM base_metrics
)

SELECT * FROM flagged_queries