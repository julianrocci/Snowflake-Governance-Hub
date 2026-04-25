/*
    Intermediate Model: Freshness & Gaps Monitoring
    This model creates a 24-hour time spine and compares it against 
    actual query history to detect data loss (gaps) or excessive latency.
*/

{{ config(
    materialized='view'
) }}

WITH timerange AS (
    -- Define the 24-hour rolling window based on the current hour
    SELECT 
        DATE_TRUNC('hour', CURRENT_TIMESTAMP()) - INTERVAL '24 hours' AS start_check,
        DATE_TRUNC('hour', CURRENT_TIMESTAMP()) AS end_check
),

hours_spine AS (
    -- Generate 25 points to ensure we cover 24 hourly intervals
    SELECT 
        DATEADD('hour', SEQ4(), (SELECT start_check FROM timerange)) AS check_hour
    FROM TABLE(GENERATOR(ROWCOUNT => 25))
),

actual_data AS (
    -- Aggregate real activity from staging
    SELECT 
        DATE_TRUNC('hour', start_time) AS event_hour,
        COUNT(*) AS n_queries
    FROM {{ ref('stg_snowflake_query_history') }}
    WHERE start_time >= (SELECT start_check FROM timerange)
    GROUP BY event_hour
),

joined AS (
    -- Confront spine with actual data
    SELECT
        h.check_hour,
        COALESCE(d.n_queries, 0) AS n_queries,
        TIMESTAMPDIFF('minute', h.check_hour, CURRENT_TIMESTAMP()) AS minutes_ago
    FROM hours_spine h
    LEFT JOIN actual_data d ON h.check_hour = d.event_hour
    WHERE h.check_hour <= (SELECT end_check FROM timerange)
)

SELECT
    check_hour,
    n_queries,
    minutes_ago,
    CASE 
        -- Healthy: We have meaningful activity
        WHEN n_queries >= 10 THEN 'HEALTHY'
        
        -- Warning: Activity is very low, but it's too recent to be sure
        WHEN n_queries < 10 AND minutes_ago <= 180 THEN 'EXPECTED_LAG'
        
        -- Gap: Very low activity and we should have received everything by now
        WHEN n_queries < 10 AND minutes_ago > 180 THEN 'GAP_DETECTED'
        
        ELSE 'UNKNOWN'
    END AS status
FROM joined