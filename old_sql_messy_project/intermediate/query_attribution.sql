-- Resolves cost attribution dimensions (team, workload, environment)
-- for each query using a warehouse → query → user tag priority model.

SELECT
    q.query_id,
    q.warehouse_name,

    COALESCE(
        wh.team,
        qt.team,
        ut.team
    ) AS team,

    COALESCE(
        wh.workload,
        qt.workload,
        ut.workload
    ) AS workload,

    COALESCE(
        wh.environment,
        qt.environment,
        ut.environment
    ) AS environment

FROM {{ ref('query_activity_enriched') }} q

LEFT JOIN {{ ref('warehouse_tags') }} wh
    ON q.warehouse_name = wh.warehouse_name

LEFT JOIN {{ ref('query_tags') }} qt
    ON q.query_id = qt.query_id

LEFT JOIN {{ ref('user_tags') }} ut
    ON q.user_name = ut.user_name