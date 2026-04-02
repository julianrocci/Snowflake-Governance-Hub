-- Attributes warehouse-level compute costs to business dimensions
-- (team, workload, environment) using warehouse tags.
-- This model represents the most realistic level of cost governance.

WITH warehouse_costs AS (

    SELECT
        warehouse_name,
        usage_date,
        billed_seconds,
        idle_seconds,
        utilization_ratio
    FROM {{ ref('warehouse_cost_model') }}

),

warehouse_tags AS (

    SELECT
        warehouse_name,

        MAX(CASE WHEN tag_name = 'team' THEN tag_value END)        AS team,
        MAX(CASE WHEN tag_name = 'workload' THEN tag_value END)    AS workload,
        MAX(CASE WHEN tag_name = 'environment' THEN tag_value END) AS environment

    FROM {{ ref('stg_warehouse_tags') }}
    GROUP BY warehouse_name

)

SELECT
    c.usage_date,
    c.warehouse_name,

    COALESCE(t.team, 'unassigned')           AS team,
    COALESCE(t.workload, 'unknown')          AS workload,
    COALESCE(t.environment, 'unknown')       AS environment,

    c.billed_seconds,
    c.idle_seconds,
    c.utilization_ratio

FROM warehouse_costs c
LEFT JOIN warehouse_tags t
    ON c.warehouse_name = t.warehouse_name
;