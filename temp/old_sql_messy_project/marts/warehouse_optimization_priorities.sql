-- Warehouse Optimization Priorities
-- Ranks warehouses by potential cost impact

WITH base AS (

    SELECT *
    FROM {{ ref('warehouse_optimization_simulation') }}

)

SELECT
    warehouse_name,
    workload,
    usage_date,

    primary_waste_driver,
    optimization_action,

    total_billed_seconds,
    simulated_billed_seconds_after_optimization,

    simulated_waste_reduction_seconds,
    simulated_cost_reduction_ratio,

    /* Priority score */
    simulated_waste_reduction_seconds
        * simulated_cost_reduction_ratio
        AS optimization_priority_score,

    /* Ranking */
    RANK() OVER (
        ORDER BY simulated_waste_reduction_seconds DESC
    ) AS optimization_rank

FROM base
ORDER BY optimization_rank;