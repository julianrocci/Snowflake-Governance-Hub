/* Mart: Warehouse Behavior Analysis.
   Focuses on identifying inefficient query patterns (Wakeups & Isolated queries). */

{{ config(
    materialized='table',
    schema='mart_cost'
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
    
    -- Domain Mapping
    CASE 
        WHEN warehouse_name LIKE 'FIN_%' THEN 'FINANCE'
        WHEN warehouse_name LIKE 'MKT_%' THEN 'MARKETING'
        WHEN warehouse_name LIKE 'ECO_%' THEN 'ECOMMERCE'
        WHEN warehouse_name LIKE 'RET_%' THEN 'RETAIL'
        WHEN warehouse_name LIKE 'ANA_%' THEN 'ANALYTICS'
        WHEN warehouse_name LIKE 'TRANSFORM_%' THEN 'DATA_ENG'
        ELSE 'OTHER'
    END AS domain,

    -- Metrics
    COUNT(query_id) AS total_queries,
    SUM(execution_time_seconds) AS total_execution_time_seconds,
    
    -- Behavior Counters (using COUNT_IF for Snowflake optimization)
    COUNT_IF(is_potential_wakeup = TRUE) AS total_wakeups,
    COUNT_IF(is_isolated_expensive_query = TRUE) AS total_isolated_queries

FROM behavior_base

GROUP BY 
    warehouse_name, 
    user_name, 
    role_name, 
    execution_date,
    domain