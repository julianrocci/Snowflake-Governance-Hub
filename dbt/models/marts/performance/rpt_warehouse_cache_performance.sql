/* Mart: Warehouse Cache Performance.
   Analyzes the distribution of query types (Result Cache, Local, Remote) 
   to monitor compute efficiency.
*/

{{ config(
    materialized='table'
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

        -- %tage of queries served by the Result Cache (cost = 0)
        ROUND(SUM(CASE WHEN cache_category = 'RESULT_CACHE' THEN 1 ELSE 0 END) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_result_cache,

        -- %tage of queries with high efficiency (over 50% from Local SSD)
        ROUND(SUM(CASE WHEN cache_category = 'LOCAL_EFFICIENT' THEN 1 ELSE 0 END) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_local_efficient,

        -- %tage of queries hitting Remote Storage (over 80% from Remote SSD)
        ROUND(SUM(CASE WHEN cache_category = 'REMOTE_HEAVY' THEN 1 ELSE 0 END) / NULLIF(COUNT(query_id), 0) * 100, 2) AS pct_remote_heavy
    FROM int_perf
    GROUP BY warehouse_name
)

SELECT 
    *,
    {{ get_domain_from_warehouse('warehouse_name') }} AS domain
FROM warehouse_aggregation
ORDER BY domain, warehouse_name