-- Aggregates warehouse wake-up events at a daily level.
-- A wake-up is defined as the first query executed
-- after a warehouse was suspended.

SELECT
    warehouse_name,
    CAST(start_time AS DATE) AS usage_date,
    COUNT(*) AS wakeup_count
FROM {{ ref('warehouse_wakeups') }}
WHERE is_wakeup = 1
GROUP BY
    warehouse_name,
    CAST(start_time AS DATE);