-- Purpose:
-- Normalize Snowflake warehouse metering data.
-- One row per warehouse billing interval.

SELECT
    warehouse_name,
    start_time,
    end_time,
    DATEDIFF('second', start_time, end_time) AS billed_seconds,
    credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP);