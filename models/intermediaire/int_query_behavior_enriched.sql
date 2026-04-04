/* Intermediate: Behavioral analysis of warehouse activity. 
   Identifies wakeups, idle gaps, and isolated queries. */

{{ config(
    materialized='ephemeral'
) }}

WITH base_queries AS (
    SELECT
        query_id,
        warehouse_name,
        user_name,
        role_name,
        start_time,
        end_time,
        execution_time_seconds
    FROM {{ ref('stg_query_history') }}
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

enriched_behavior AS (
    SELECT
        *,
        -- Time gap in seconds since the previous query
        DATEDIFF('second', previous_query_end_time, start_time) AS idle_gap_seconds,

        -- Flags for interpretation (potential first query of the 30D window )
        CASE
            WHEN previous_query_end_time IS NULL THEN TRUE
            ELSE FALSE
        END AS is_first_query_in_window,

        -- Identify the potential wakeup (admiting auto-suspend = 60s)
        CASE 
            WHEN previous_query_end_time IS NULL THEN TRUE
            WHEN DATEDIFF('second', previous_query_end_time, start_time) >= 60 THEN TRUE
            ELSE FALSE 
        END AS is_potential_wakeup,

        -- Shorts queries (<5) with big gap (>60)
        CASE 
            WHEN execution_time_seconds < 5 
                 AND (previous_query_end_time IS NULL OR DATEDIFF('second', previous_query_end_time, start_time) >= 60)
            THEN TRUE 
            ELSE FALSE 
        END AS is_isolated_expensive_query
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
    idle_gap_seconds,
    is_first_query_in_window,
    is_potential_wakeup,
    is_isolated_expensive_query
FROM enriched_behavior