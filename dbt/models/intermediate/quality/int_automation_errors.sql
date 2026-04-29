/*
    Intermediate Model: Automation Failures Monitoring
    Focus: Identifies failed executions from automated processes in the last 30 days.
*/

{{ config(
    materialized='view'
) }}

WITH failed_queries AS (
    SELECT
        query_id,
        query_text,
        user_name,
        warehouse_name,
        execution_status,
        error_code,
        error_message,
        start_time,
        end_time,
        total_elapsed_time / 1000 AS duration_seconds
    FROM {{ ref('stg_snowflake_query_history') }}
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND execution_status NOT IN ('SUCCESS', 'RUNNING')
),

categorized_failures AS (
    -- Define categories based on naming conventions and SQL patterns
    SELECT
        *,
        CASE 
            WHEN user_name ILIKE '%SYSTEM%' OR query_text ILIKE '%EXECUTE TASK%' THEN 'TASK'
            WHEN query_text ILIKE '%COPY INTO%' OR query_text ILIKE '%SNOWPIPE%' THEN 'INGESTION_PIPE'
            WHEN user_name ILIKE '%DBT%' OR user_name ILIKE '%SERVICE%' THEN 'TRANSFORMATION_JOB'
            ELSE 'OTHER_AUTOMATION_FAILURE'
        END AS automation_type
    FROM failed_queries
)

SELECT
    *
FROM categorized_failures
WHERE automation_type IN ('TASK', 'INGESTION_PIPE', 'TRANSFORMATION_JOB')