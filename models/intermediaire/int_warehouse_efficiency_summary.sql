/* Intermediate: Global 30-day efficiency per warehouse (Compute Only).
   Aggregates compute credits to measure warehouse optimization. */


{{ config(
    materialized='ephemeral'
) }}


WITH global_billing AS (
    SELECT
        warehouse_name,
        -- We isolate Compute credits to avoid Cloud Services noise
        SUM(credits_used_compute) AS total_compute_credits,
        -- Total billed time: 1 compute credit = 3600 seconds of an X-Small WH
        SUM(credits_used_compute) * 3600 AS total_billed_seconds
    FROM {{ ref('stg_warehouse_metering') }}
    GROUP BY warehouse_name
),

global_execution AS (
    SELECT
        warehouse_name,
        SUM(execution_time_seconds) AS total_work_seconds,
        COUNT(query_id) AS total_queries_executed
    FROM {{ ref('stg_query_history') }}
    GROUP BY warehouse_name
)

-- Final Join: One row per Warehouse for the last 30 days

SELECT
    b.warehouse_name,
    b.total_compute_credits,
    b.total_billed_seconds,
    NVL(e.total_work_seconds, 0) AS total_work_seconds,
    NVL(e.total_queries_executed, 0) AS total_queries_executed,
    -- Effective compute cost (USD)
    (b.total_compute_credits * 4.0) AS total_compute_cost_usd
FROM global_billing b
LEFT JOIN global_execution e
    ON b.warehouse_name = e.warehouse_name