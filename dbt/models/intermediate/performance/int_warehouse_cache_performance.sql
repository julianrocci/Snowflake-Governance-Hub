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
            WHEN percentage_scanned_from_cache = 1 THEN 'RESULT_CACHE'
            WHEN percentage_scanned_from_cache > 0.50 THEN 'LOCAL_EFFICIENT'
            WHEN percentage_scanned_from_cache < 0.10 AND total_bytes > 0 THEN 'REMOTE_HEAVY'
            ELSE 'MIXED'
        END AS cache_category
    FROM base_metrics
)

SELECT * FROM flagged_queries