/* Mart: Warehouse Cache Performance.
   Analyzes the distribution of query types (Result Cache, Local, Remote) 
   to monitor compute efficiency.
*/

{{ config(
    materialized='table',
    schema='mart_performance'
) }}

WITH int_perf AS (
    SELECT * FROM {{ ref('int_warehouse_cache_performance') }}
),

warehouse_aggregation AS (
    SELECT
        warehouse_name,
        COUNT(query_id) AS total_query_count,
        
        -- Conversion from Bytes to Gigabytes (1024^3)
        ROUND(SUM(total_bytes) / POWER(1024, 3), 2) AS total_gb_scanned,
        ROUND(AVG(total_elapsed_time), 2) AS avg_execution_time_ms,

        -- %tage of queries served by the Result Cache (cost = 0)
        ROUND(SUM(is_result_cache_hit) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_result_cache_hits,

        -- %tage of queries with high efficiency (over 50% from Local SSD)
        ROUND(SUM(is_local_efficient) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_local_disk_efficient,

        -- %tage of queries with poor efficiency (over 50% from Remote Storage)
        ROUND(SUM(is_remote_heavy) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_remote_disk_heavy

    FROM int_perf
    GROUP BY warehouse_name
)

SELECT 
    *,
    -- By Business domain
    CASE 
        WHEN warehouse_name LIKE 'FIN_%' THEN 'FINANCE'
        WHEN warehouse_name LIKE 'MKT_%' THEN 'MARKETING'
        WHEN warehouse_name LIKE 'ECO_%' THEN 'ECOMMERCE'
        WHEN warehouse_name LIKE 'RET_%' THEN 'RETAIL'
        WHEN warehouse_name LIKE 'ANA_%' THEN 'ANALYTICS'
        WHEN warehouse_name LIKE 'TRANSFORM_%' THEN 'DATA_ENG'
        ELSE 'OTHER'
    END AS domain

FROM warehouse_aggregation
ORDER BY domain, avg_execution_time_ms DESC