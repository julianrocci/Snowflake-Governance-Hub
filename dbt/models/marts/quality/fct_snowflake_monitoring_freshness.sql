/*
    Fact Table: Snowflake Data Freshness & Gaps
    This model provides a clean view for dashboards to visualize 
    pipeline health over the last 24 hours.
*/

{{ config(
    materialized='table'
) }}

WITH freshness_data AS (
    -- We pull from our intermediate model
    SELECT
        check_hour,
        n_queries,
        minutes_ago,
        status
    FROM {{ ref('int_monitoring_freshness_gaps') }}
)

SELECT
    check_hour,
    n_queries,
    status,
    -- Simple flag for easier filtering in BI tools
    CASE 
        WHEN status = 'GAP_DETECTED' THEN 1 
        ELSE 0 
    END AS is_gap,
    -- Human readable latency
    CASE 
        WHEN minutes_ago < 60 THEN minutes_ago || ' mins ago'
        ELSE ROUND(minutes_ago / 60, 1) || ' hours ago'
    END AS time_since_check,
    -- Metadata for auditing
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM freshness_data
ORDER BY check_hour DESC