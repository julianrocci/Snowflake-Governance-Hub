-- Warehouse Optimization Simulation Layer
-- Simulates cost impact after applying recommended optimizations

WITH base AS (

    SELECT *
    FROM {{ ref('warehouse_optimization_model') }}

),

simulation AS (

    SELECT
        *,

        /* ==========================================
           Simulated billed seconds after optimization
           ========================================== */

        CASE

            WHEN primary_waste_driver = 'IDLE_DOMINANT'
                THEN total_billed_seconds 
                     - (total_billed_seconds * idle_ratio * 0.50)

            WHEN primary_waste_driver = 'UNDERUTILIZED'
                THEN total_billed_seconds * 0.70

            WHEN primary_waste_driver = 'MULTICLUSTER_OVERPROVISIONED'
                THEN total_active_query_seconds * 1.10

            ELSE total_billed_seconds

        END AS simulated_billed_seconds_after_optimization

    FROM base
)

SELECT
    *,

    /* ==========================================
       Simulated waste reduction (seconds)
       ========================================== */

    total_billed_seconds 
    - simulated_billed_seconds_after_optimization
    AS simulated_waste_reduction_seconds,

    /* ==========================================
       Simulated cost reduction ratio
       ========================================== */

    CASE
        WHEN total_billed_seconds > 0
        THEN
            (
                total_billed_seconds
                - simulated_billed_seconds_after_optimization
            ) / total_billed_seconds
        ELSE 0
    END AS simulated_cost_reduction_ratio

FROM simulation
ORDER BY simulated_cost_reduction_ratio DESC;