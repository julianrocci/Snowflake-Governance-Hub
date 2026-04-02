-- Purpose:
-- Normalize Snowflake query history for cost governance analysis.
-- One row per query.

SELECT
    query_id,
    warehouse_name,
    user_name,
    start_time,
    end_time,
    DATEDIFF('second', start_time, end_time) AS execution_time_seconds,
    execution_status,
    rows_produced,
    bytes_scanned,
    query_tag
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name IS NOT NULL    -- Not serverless
  AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP);