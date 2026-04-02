-- Reconciles warehouse-level billed compute with query-level activity
-- to estimate active versus idle compute time for cost governance.

WITH metering AS (
    SELECT
        warehouse_name,
        usage_date,
        billed_seconds
    FROM {{ ref('warehouse_usage_window') }}
),

query_activity AS (
    SELECT
        warehouse_name,
        DATE(start_time) AS usage_date,
        SUM(execution_time_seconds) AS active_query_seconds
    FROM {{ ref('query_activity_enriched') }}
    GROUP BY 1, 2
)

SELECT
    m.warehouse_name,
    m.usage_date,
    m.billed_seconds,
    COALESCE(q.active_query_seconds, 0) AS active_query_seconds,
    GREATEST(
        m.billed_seconds - COALESCE(q.active_query_seconds, 0),
        0
    ) AS idle_seconds,
    ROUND(
        COALESCE(q.active_query_seconds, 0) / NULLIF(m.billed_seconds, 0),
        4
    ) AS utilization_ratio
FROM metering m
LEFT JOIN query_activity q
    ON m.warehouse_name = q.warehouse_name
   AND m.usage_date = q.usage_date