-- Reconstructs Snowflake compute billing at the warehouse level
-- using WAREHOUSE_METERING_HISTORY.
-- Provides billed compute time (in seconds) per warehouse and day
-- as the baseline for cost governance and attribution.

WITH metering AS (
    SELECT
        warehouse_name,
        DATE(start_time) AS usage_date,
        SUM(credits_used_compute) * 3600 AS billed_seconds
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
)

SELECT
    warehouse_name,
    usage_date,
    billed_seconds
FROM metering