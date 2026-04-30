    /* Mart: Warehouse Efficiency & FinOps ROI.
       This is the final table for the Cost Governance Dashboard. */
    
    {{ config(
        materialized='table'
    ) }}
    
    WITH efficiency_base AS (
        SELECT * FROM {{ ref('int_warehouse_efficiency_summary') }}
    ),
    
    final_calculations AS (
        SELECT
            warehouse_name,
            total_compute_credits,
            total_compute_cost_usd,
            total_billed_seconds,
            total_work_seconds,
            total_queries_executed,
            
            -- Efficiency Ratio: (Real Work / Billed Time)
            COALESCE(
                ROUND(
                    (total_work_seconds / NULLIF(total_billed_seconds, 0)) * 100, 
                    2
                ), 0
            ) AS efficiency_percentage,
            
            -- Idle Cost
            ROUND(
                total_compute_cost_usd * (1 - (total_work_seconds / NULLIF(total_billed_seconds, 0))),
                2
            ) AS wasted_cost_usd
    
        FROM efficiency_base
    )
    
    SELECT 
        *,
        {{ get_environment_from_name('warehouse_name') }} AS environment,
    {{ get_domain_from_warehouse('warehouse_name') }} AS domain
    FROM final_calculations