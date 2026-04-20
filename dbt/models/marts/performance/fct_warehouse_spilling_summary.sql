/* Mart: Warehouse Spilling Summary.
   Aggregates spilling incidents by domain and warehouse to monitor 
   memory efficiency and resource sizing.
*/

{{ config(
    materialized='table'
) }}

WITH int_spilling AS (
    SELECT * FROM {{ ref('int_warehouse_spilling_performance') }}
),

warehouse_aggregation AS (
    SELECT
        warehouse_name,
        -- Global statistics
        COUNT(query_id) AS total_query_count,
        ROUND(SUM(total_bytes) / POWER(1024, 3), 2) AS total_gb_scanned,
        ROUND(AVG(total_elapsed_time), 2) AS avg_execution_time_ms,

        -- Bad Local Spilling Ratio (Queries with > 30% Local Spill)
        ROUND(
            SUM(is_bad_local_spill) / NULLIF(COUNT(query_id), 0) * 100, 
            2
        ) AS pct_bad_local_spilling,

        -- Critical Remote Spilling Ratio (Queries with > 1% Remote Spill)
        ROUND(
            SUM(is_critical_remote_spill) / NULLIF(COUNT(query_id), 0) * 100, 
            2
        ) AS pct_critical_remote_spilling
    FROM int_spilling
    GROUP BY warehouse_name
)

SELECT 
    *,
    {{ get_domain_from_warehouse('warehouse_name') }} AS domain
FROM warehouse_aggregation
ORDER BY domain, pct_critical_remote_spilling DESC