-- Warehouse Optimization Model
-- Includes optimization recommendation, waste estimation,
-- and primary waste driver classification

WITH base AS (

    SELECT
        warehouse_name,
        workload,
        usage_date,

        weighted_utilization_ratio,
        idle_ratio,
        wakeup_ratio,

        total_billed_seconds,
        total_active_query_seconds,

        estimated_savings_dollars,
        projected_monthly_savings,
        projected_annual_savings

    FROM {{ ref('warehouse_optimization_recommendation') }}
),

optimization_logic AS (

    SELECT
        warehouse_name,
        workload,
        usage_date,

        weighted_utilization_ratio,
        idle_ratio,
        wakeup_ratio,

        total_billed_seconds,
        total_active_query_seconds,

        estimated_savings_dollars,
        projected_monthly_savings,
        projected_annual_savings,

        /* ===============================
           Optimization Action
           =============================== */

        CASE

            WHEN weighted_utilization_ratio < 0.30
                THEN 'DOWNSIZE_WAREHOUSE'

            WHEN idle_ratio > 0.50
                THEN 'REDUCE_AUTO_SUSPEND_TIMEOUT'

            WHEN weighted_utilization_ratio > 0.60
                 AND idle_ratio < 0.20
                 AND total_billed_seconds > total_active_query_seconds * 1.5
                THEN 'REVIEW_MULTI_CLUSTER_CONFIGURATION'

            ELSE 'HEALTHY'

        END AS optimization_action,

        /* ===============================
           Estimated Waste (Seconds)
           =============================== */

        CASE

            WHEN weighted_utilization_ratio < 0.30
                THEN total_billed_seconds * 0.30

            WHEN idle_ratio > 0.50
                THEN total_billed_seconds * 0.20

            WHEN weighted_utilization_ratio > 0.60
                 AND idle_ratio < 0.20
                 AND total_billed_seconds > total_active_query_seconds * 1.5
                THEN (total_billed_seconds - total_active_query_seconds)

            ELSE 0

        END AS estimated_waste_seconds

    FROM base
),

final_model AS (

    SELECT
        *,

        /* ===============================
           Estimated Waste Ratio
           =============================== */

        CASE
            WHEN total_billed_seconds > 0
                THEN estimated_waste_seconds / total_billed_seconds
            ELSE 0
        END AS estimated_waste_ratio,

        /* ===============================
           Primary Waste Driver
           =============================== */

        CASE

            WHEN idle_ratio > 0.50
                THEN 'IDLE_DOMINANT'

            WHEN weighted_utilization_ratio < 0.30
                THEN 'UNDERUTILIZED'

            WHEN total_billed_seconds > total_active_query_seconds * 1.5
                 AND idle_ratio < 0.20
                THEN 'MULTICLUSTER_OVERPROVISIONED'

            WHEN (
                    CASE
                        WHEN total_billed_seconds > 0
                            THEN estimated_waste_seconds / total_billed_seconds
                        ELSE 0
                    END
                 ) > 0.25
                THEN 'MIXED'

            ELSE 'HEALTHY'

        END AS primary_waste_driver

    FROM optimization_logic
)

SELECT *
FROM final_model
ORDER BY estimated_waste_ratio DESC;