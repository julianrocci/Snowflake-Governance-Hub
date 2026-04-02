-- Expose normalized warehouse efficiency metrics
-- including utilization, idle time, wakeup behavior,
-- and multi-cluster activity signals.

WITH query_wakeup_stats AS (
    SELECT
        warehouse_name,
        DATE(start_time) AS usage_date,

        COUNT(*) AS total_query_count,
        SUM(is_wakeup) AS wakeup_query_count

    FROM {{ ref('warehouse_query_wakeups') }}
    GROUP BY warehouse_name, DATE(start_time)
),

cluster_activity AS (
    SELECT
        warehouse_name,
        DATE(start_time) AS usage_date,

        AVG(avg_running) AS avg_cluster_count,
        MAX(max_running) AS max_cluster_count_seen

    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    GROUP BY warehouse_name, DATE(start_time)
)

SELECT
    c.warehouse_name,
    c.workload,
    c.usage_date,

    -- Core cost metrics
    c.total_billed_seconds,
    c.total_active_query_seconds,
    c.total_idle_seconds,

    -- Efficiency ratios
    c.weighted_utilization_ratio,

    ROUND(
        c.total_idle_seconds / NULLIF(c.total_billed_seconds, 0),
        4
    ) AS idle_ratio,

    -- Wakeup metrics
    q.total_query_count,
    q.wakeup_query_count,

    ROUND(
        q.wakeup_query_count / NULLIF(q.total_query_count, 0),
        4
    ) AS wakeup_ratio,

    -- NEW: Multi-cluster metrics
    ca.avg_cluster_count,
    ca.max_cluster_count_seen,

    CASE
        WHEN ca.avg_cluster_count > 1.2 THEN TRUE
        ELSE FALSE
    END AS is_multi_cluster_active

FROM {{ ref('warehouse_cost_attribution') }} c

LEFT JOIN query_wakeup_stats q
    ON c.warehouse_name = q.warehouse_name
   AND c.usage_date = q.usage_date

LEFT JOIN cluster_activity ca
    ON c.warehouse_name = ca.warehouse_name
   AND c.usage_date = ca.usage_date;
