/* Mart: Warehouse Efficiency & FinOps ROI.
   This is the final table for the Cost Governance Dashboard. */

{{ config(
    materialized='table',
    schema='mart_cost'
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
        ROUND(
            (total_work_seconds / NULLIF(total_billed_seconds, 0)) * 100, 
            2
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
    -- By Environment
    CASE 
        WHEN warehouse_name LIKE '%_PROD' OR warehouse_name LIKE 'PROD_%' THEN 'PROD'
        WHEN warehouse_name LIKE '%_UAT'  OR warehouse_name LIKE 'UAT_%'  THEN 'UAT'
        WHEN warehouse_name LIKE '%_DEV'  OR warehouse_name LIKE 'DEV_%'  THEN 'DEV'
        ELSE 'OTHER'
    END AS environment,

    -- By Workload type
    CASE 
        WHEN warehouse_name LIKE '%_LOAD_%' OR warehouse_name LIKE '%_INGEST_%' THEN 'DATA_INGESTION'
        WHEN warehouse_name LIKE '%_TRANSFORM_%' OR warehouse_name LIKE '%_DBT_%' THEN 'TRANSFORMATION'
        WHEN warehouse_name LIKE '%_BI_%' OR warehouse_name LIKE '%_REPORT_%' THEN 'REPORTING/BI'
        ELSE 'GENERAL_PURPOSE'
    END AS warehouse_type,

    -- By Business domain
    CASE 
        WHEN warehouse_name LIKE '%FIN_%' THEN 'FINANCE'
        WHEN warehouse_name LIKE '%MKT_%' THEN 'MARKETING'
        WHEN warehouse_name LIKE '%ECO_%' THEN 'ECOMMERCE'
        WHEN warehouse_name LIKE '%RET_%' THEN 'RETAIL'
        WHEN warehouse_name LIKE '%ANA_%' THEN 'ANALYTICS'
        WHEN warehouse_name LIKE '%TRANSFORM_%' THEN 'DATA_ENG'
        ELSE 'OTHER'
    END AS domain
FROM final_calculations