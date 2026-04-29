/*
    Fact Table: Data Quality & Automation Monitoring
    Consolidates data freshness, volume gaps, and technical failures 
    into a single view.
*/

{{ config(
    materialized='table'
) }}

WITH freshness_gaps AS (
    -- Get volume and freshness metrics
    SELECT
        check_hour AS event_timestamp,
        'DATA_FLOW' AS category,
        status AS alert_level,
        total_rows_inserted,
        n_queries,
        CASE 
            WHEN status = 'GAP_DETECTED' THEN 'No data received for > 3 hours'
            WHEN status = 'SILENT_FAILURE' THEN 'Queries running but 0 rows inserted'
            ELSE 'Normal operations'
        END AS event_description,
        NULL AS error_message
    FROM {{ ref('int_monitoring_freshness_gaps') }}
),

automation_failures AS (
    -- Get technical execution errors
    SELECT
        start_time AS event_timestamp,
        'TECHNICAL_ERROR' AS category,
        automation_type AS alert_level,
        NULL AS total_rows_inserted,
        1 AS n_queries,
        'Failed ' || automation_type || ': ' || SUBSTR(query_text, 1, 50) AS event_description,
        error_message
    FROM {{ ref('int_automation_errors') }}
),

unioned AS (
    SELECT * FROM freshness_gaps
    UNION ALL
    SELECT * FROM automation_failures
)

SELECT
    event_timestamp,
    category,
    alert_level,
    total_rows_inserted,
    n_queries,
    event_description,
    error_message,
    -- Filtering: 1 for any issue, 0 for healthy status
    CASE 
        WHEN alert_level IN ('GAP_DETECTED', 'SILENT_FAILURE', 'TASK', 'INGESTION_PIPE', 'TRANSFORMATION_JOB') THEN 1
        ELSE 0 
    END AS is_alert,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM unioned
ORDER BY event_timestamp DESC