-- Identifies warehouses with inefficient compute usage patterns
-- by combining billed usage, idle time, and wake-up behavior.
-- This model highlights optimization candidates for cost governance.

WITH warehouse_metrics AS (

    SELECT
        c.warehouse_name,
        c.team,
        c.workload,
        c.environment,

        COUNT(DISTINCT c.usage_date)                   AS active_days,

        SUM(c.billed_seconds)                          AS total_billed_seconds,
        SUM(c.idle_seconds)                            AS total_idle_seconds,

        -- Weighted utilization across the full period
        SUM(c.billed_seconds - c.idle_seconds)
            / NULLIF(SUM(c.billed_seconds), 0)         AS weighted_utilization_ratio,

        SUM(COALESCE(w.wakeup_count, 0))               AS wakeup_count

    FROM {{ ref('warehouse_cost_attribution') }} c
    LEFT JOIN {{ ref('warehouse_daily_wakeups') }} w
        ON c.warehouse_name = w.warehouse_name
       AND c.usage_date = w.usage_date
    GROUP BY 1,2,3,4

)

SELECT
    warehouse_name,
    team,
    workload,
    environment,

    active_days,
    total_billed_seconds,
    total_idle_seconds,
    weighted_utilization_ratio,
    wakeup_count,

    -- Efficiency flags
    CASE
        WHEN weighted_utilization_ratio < 0.30 THEN 1
        ELSE 0
    END AS low_utilization_flag,

    CASE
        WHEN wakeup_count > active_days * 3 THEN 1
        ELSE 0
    END AS high_wakeup_flag,

    CASE
        WHEN weighted_utilization_ratio < 0.30
          OR wakeup_count > active_days * 3
        THEN 1
        ELSE 0
    END AS optimization_candidate_flag,

    -- Conservative estimate of reclaimable idle compute
    ROUND(total_idle_seconds * 0.5) AS potential_idle_savings_seconds

FROM warehouse_metrics;
