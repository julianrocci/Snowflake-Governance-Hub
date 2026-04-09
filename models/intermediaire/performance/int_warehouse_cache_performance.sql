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
        percentage_scanned_from_cache,
        bytes_scanned_from_local_storage AS bytes_local,
        bytes_scanned_from_remote_storage AS bytes_remote,
        (bytes_scanned_from_local_storage + bytes_scanned_from_remote_storage) AS total_bytes
    FROM {{ ref('stg_snowflake_query_history') }}
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      -- Exclude Cloud Services only queries (Metadata only queries)
      AND warehouse_size IS NOT NULL
),

flagged_queries AS (
    SELECT
        *,
        -- Result Cache: Full match in Snowflake Service Layer
        CASE 
            WHEN percentage_scanned_from_cache = 1 THEN 1 
            ELSE 0 
        END AS is_result_cache_hit,
        
        -- Local Disk Efficiency: > 50% data from SSD
        CASE 
            WHEN percentage_scanned_from_cache < 1 
                 AND (bytes_local / NULLIF(total_bytes, 0)) > 0.5 
            THEN 1 ELSE 0 
        END AS is_local_efficient,
        
        -- Remote Disk Heavy: > 50% data from Cloud Storage (S3/Azure)
        CASE 
            WHEN percentage_scanned_from_cache < 1 
                 AND (bytes_remote / NULLIF(total_bytes, 0)) >= 0.5 
            THEN 1 ELSE 0 
        END AS is_remote_heavy
    FROM base_metrics
)

SELECT * FROM flagged_queries