/* Intermediate: Caching Performance Metrics.
   Calculates hit ratios for Result Cache and Local Disk Cache. */

{{ config(
    materialized='ephemeral'
) }}

WITH query_source AS (
    SELECT
        query_id,
        warehouse_name,
        start_time,
        total_elapsed_time,
        -- Result Cache
        percentage_scanned_from_cache AS result_cache_hit_percentage,
        
        -- Data volumes
        bytes_scanned_from_local_storage AS bytes_local,
        bytes_scanned_from_remote_storage AS bytes_remote,
        (bytes_scanned_from_local_storage + bytes_scanned_from_remote_storage) AS total_bytes_scanned
    FROM {{ ref('stg_snowflake_query_history') }}
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND warehouse_size IS NOT NULL -- Focus on compute queries
),

cache_calculations AS (
    SELECT
        *,
        -- Local Disk Cache Ratio: Avoiding division by zero
        CASE 
            WHEN total_bytes_scanned = 0 THEN 0
            ELSE (bytes_local / total_bytes_scanned)
        END AS local_disk_cache_ratio,
        
        -- Remote Scan Ratio
        CASE 
            WHEN total_bytes_scanned = 0 THEN 0
            ELSE (bytes_remote / total_bytes_scanned)
        END AS remote_disk_scan_ratio
    FROM query_source
)

SELECT * FROM cache_calculations