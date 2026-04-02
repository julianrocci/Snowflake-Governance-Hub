-- ============================================================
-- Intermediate model: Query Activity Enriched
-- ------------------------------------------------------------
-- Purpose:
-- Enrich Snowflake QUERY_HISTORY with sequencing logic
-- to analyze warehouse behavior, wake-ups, and idle gaps.
-- This model does NOT compute costs. It focuses on behavior.
-- ============================================================

WITH base_queries AS (

    SELECT
        query_id,
        warehouse_name,
        user_name,
        role_name,
        start_time,
        end_time,

        -- Execution time in seconds (Snowflake provides milliseconds)
        execution_time / 1000 AS execution_time_seconds

    FROM snowflake.account_usage.query_history

    WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
      AND warehouse_name IS NOT NULL
),

sequenced_queries AS (

    SELECT
        *,
        -- End time of the previous query on the same warehouse
        LAG(end_time) OVER (
            PARTITION BY warehouse_name
            ORDER BY start_time
        ) AS previous_query_end_time

    FROM base_queries
),

enriched_queries AS (

    SELECT
        *,
        -- Time gap in seconds since the previous query
        CASE
            WHEN previous_query_end_time IS NULL THEN NULL
            ELSE DATEDIFF(
                second,
                previous_query_end_time,
                start_time
            )
        END AS gap_seconds_since_previous_query,

        -- Flags for interpretation (potential first query of the 30D window )
        CASE
            WHEN previous_query_end_time IS NULL THEN TRUE
            ELSE FALSE
        END AS is_first_query_in_window,

        -- Identify the potential wakeup (admiting auto-suspend = 60s)
        CASE
            WHEN previous_query_end_time IS NOT NULL
             AND DATEDIFF(second, previous_query_end_time, start_time) > 60
            THEN TRUE
            ELSE FALSE
        END AS is_potential_wakeup,

        -- Shorts queries (<5) with big gap (>60)
        CASE
            WHEN execution_time / 1000 < 5
             AND previous_query_end_time IS NOT NULL
             AND DATEDIFF(second, previous_query_end_time, start_time) > 60
            THEN TRUE
            ELSE FALSE
        END AS is_isolated_query

    FROM sequenced_queries
)

SELECT
    query_id,
    warehouse_name,
    user_name,
    role_name,
    start_time,
    end_time,
    execution_time_seconds,
    previous_query_end_time,
    gap_seconds_since_previous_query,
    is_first_query_in_window,
    is_potential_wakeup,
    is_isolated_query

FROM enriched_queries;
