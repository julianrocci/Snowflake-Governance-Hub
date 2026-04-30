/* Mart: Warehouse Behavior Analysis.
   Focuses on identifying inefficient query patterns (Wakeups & Isolated queries). */

{{ config(
    materialized='table'
) }}

WITH behavior_base AS (
    SELECT * FROM {{ ref('int_query_behavior_enriched') }}
)

SELECT
    warehouse_name,
    user_name,
    role_name,
    -- Grouping by Day to see trends
    DATE_TRUNC('day', start_time) AS execution_date,
    
    {{ get_domain_from_warehouse('warehouse_name') }} AS domain,

    -- Metrics
    COUNT(query_id) AS total_queries,
    SUM(execution_time_seconds) AS total_execution_time_seconds,
    
    -- Behavior Counters (useful for alerting)
    SUM(CASE WHEN is_potential_wakeup THEN 1 ELSE 0 END) AS wakeup_count,
    SUM(CASE WHEN is_isolated_expensive_query THEN 1 ELSE 0 END) AS isolated_query_count

FROM behavior_base
GROUP BY
    warehouse_name,
    user_name,
    role_name,
    DATE_TRUNC('day', start_time),
    {{ get_domain_from_warehouse('warehouse_name') }}