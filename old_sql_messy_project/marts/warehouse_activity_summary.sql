-- ============================================================
-- Mart: Warehouse Activity Summary
-- ------------------------------------------------------------
-- Purpose:
-- Aggregate query-level activity signals at warehouse level
-- to analyze usage patterns, wake-ups, and workload fragmentation.
-- This model serves as an input for cost attribution later on.
-- ============================================================

WITH query_activity AS (

    SELECT *
    FROM analytics.intermediate.query_activity_enriched
),

warehouse_aggregates AS (

    SELECT
        warehouse_name,

        COUNT(*) AS total_queries,

        COUNT(DISTINCT user_name) AS distinct_users,

        MIN(start_time) AS first_query_time,
        MAX(end_time)   AS last_query_time,

        -- Execution time
        SUM(execution_time_seconds) AS total_execution_time_seconds,
        AVG(execution_time_seconds) AS avg_execution_time_seconds,

        -- Activity signals
        SUM(CASE WHEN is_first_query_in_window THEN 1 ELSE 0 END)
            AS first_queries_in_window,

        SUM(CASE WHEN is_potential_wakeup THEN 1 ELSE 0 END)
            AS potential_wakeups,

        SUM(CASE WHEN is_isolated_query THEN 1 ELSE 0 END)
            AS isolated_queries

    FROM query_activity
    GROUP BY warehouse_name
)

SELECT
    warehouse_name,
    total_queries,
    distinct_users,
    first_query_time,
    last_query_time,
    total_execution_time_seconds,
    avg_execution_time_seconds,
    first_queries_in_window,
    potential_wakeups,
    isolated_queries,

    -- Ratios (interpretation-friendly)
    potential_wakeups / NULLIF(total_queries, 0) AS wakeup_ratio,
    isolated_queries   / NULLIF(total_queries, 0) AS isolated_query_ratio

FROM warehouse_aggregates;
