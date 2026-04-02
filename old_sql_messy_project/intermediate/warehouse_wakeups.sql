-- Purpose:
-- Identify warehouse wake-up queries based on query gaps.
-- One row per detected wake-up event.

WITH ordered_queries AS (
    SELECT
        query_id,
        warehouse_name,
        user_name,
        start_time,
        end_time,
        execution_time_seconds,
        LAG(end_time) OVER (
            PARTITION BY warehouse_name
            ORDER BY start_time
        ) AS previous_query_end_time
    FROM {{ ref('stg_queries') }}
),

query_gaps AS (
    SELECT
        *,
        DATEDIFF(
            'second',
            previous_query_end_time,
            start_time
        ) AS gap_seconds
    FROM ordered_queries
)

SELECT
    warehouse_name,
    query_id AS wakeup_query_id,
    user_name AS wakeup_user,
    start_time AS wakeup_time,
    execution_time_seconds AS wakeup_query_duration,
    gap_seconds
FROM query_gaps
WHERE previous_query_end_time IS NULL
   OR gap_seconds >= 60;