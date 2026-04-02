-- Snowflake Warehouse Efficiency Trend Model
-- Detects rolling trends and early degradation signals
-- Based on warehouse_optimization_recommendation

WITH base AS (

    SELECT
        warehouse_name,
        workload,
        usage_date,

        weighted_utilization_ratio,
        idle_ratio,
        wakeup_ratio,

        estimated_savings_dollars,
        projected_monthly_savings,
        projected_annual_savings

    FROM {{ ref('warehouse_optimization_recommendation') }}
),

rolling_metrics AS (

    SELECT
        *,

        ------------------------------------------------------------------
        -- 7 Day Rolling Averages (Short-term signal)
        ------------------------------------------------------------------
        AVG(weighted_utilization_ratio) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_util_7d,

        AVG(idle_ratio) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_idle_7d,

        AVG(estimated_savings_dollars) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_savings_7d,

        ------------------------------------------------------------------
        -- 30 Day Rolling Averages (Baseline signal)
        ------------------------------------------------------------------
        AVG(weighted_utilization_ratio) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_util_30d,

        AVG(idle_ratio) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_idle_30d,

        AVG(estimated_savings_dollars) OVER (
            PARTITION BY warehouse_name
            ORDER BY usage_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_savings_30d

    FROM base
),

trend_analysis AS (

    SELECT
        *,

        ------------------------------------------------------------------
        -- Short-term vs Long-term Delta
        ------------------------------------------------------------------
        avg_util_7d - avg_util_30d AS util_delta,
        avg_idle_7d - avg_idle_30d AS idle_delta,
        avg_savings_7d - avg_savings_30d AS savings_delta,

        ------------------------------------------------------------------
        -- Trend Alerts
        ------------------------------------------------------------------
        CASE
            WHEN avg_util_7d < avg_util_30d * 0.85
                THEN 'UTILIZATION_DROPPING'

            WHEN avg_idle_7d > avg_idle_30d * 1.20
                THEN 'IDLE_INCREASING'

            WHEN avg_savings_7d > avg_savings_30d * 1.30
                THEN 'COST_WORSENING'

            ELSE NULL
        END AS trend_alert,

        ------------------------------------------------------------------
        -- Alert Severity
        ------------------------------------------------------------------
        CASE
            WHEN avg_savings_7d > avg_savings_30d * 1.50
                THEN 'CRITICAL'

            WHEN avg_savings_7d > avg_savings_30d * 1.30
                THEN 'HIGH'

            WHEN avg_idle_7d > avg_idle_30d * 1.20
                THEN 'MEDIUM'

            WHEN avg_util_7d < avg_util_30d * 0.85
                THEN 'LOW'

            ELSE 'NONE'
        END AS alert_severity

    FROM rolling_metrics
)

SELECT *
FROM trend_analysis;