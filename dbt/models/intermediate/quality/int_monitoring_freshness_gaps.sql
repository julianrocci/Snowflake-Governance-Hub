/*
    Intermediate Model: Freshness & Volume Gaps Monitoring
    Generates a 24-hour spine and compares it with 'rows_inserted' 
    to detect periods where no data was actually loaded.
*/

{{ config(
    materialized='view'
) }}

WITH timerange AS (
    -- Define the 24-hour rolling window
    SELECT 
        DATE_TRUNC('hour', CURRENT_TIMESTAMP()) - INTERVAL '24 hours' AS start_check,
        DATE_TRUNC('hour', CURRENT_TIMESTAMP()) AS end_check
),

hours_spine AS (
    -- Generate 25 points to cover the 24-hour range
    SELECT 
        DATEADD('hour', SEQ4(), (SELECT start_check FROM timerange)) AS check_hour
    FROM TABLE(GENERATOR(ROWCOUNT => 25))
),

actual_data AS (
    -- Aggregate real volume from staging
    SELECT 
        DATE_TRUNC('hour', start_time) AS event_hour,
        -- Summing the actual data volume
        SUM(rows_inserted) AS total_rows_inserted,
        COUNT(query_id) AS n_queries
    FROM {{ ref('stg_snowflake_query_history') }}
    WHERE start_time >= (SELECT start_check FROM timerange)
    GROUP BY event_hour
),

joined AS (
    -- Confront spine with volume data
    SELECT
        h.check_hour,
        COALESCE(d.total_rows_inserted, 0) AS total_rows_inserted,
        COALESCE(d.n_queries, 0) AS n_queries,
        TIMESTAMPDIFF('minute', h.check_hour, CURRENT_TIMESTAMP()) AS minutes_ago
    FROM hours_spine h
    LEFT JOIN actual_data d ON h.check_hour = d.event_hour
    WHERE h.check_hour <= (SELECT end_check FROM timerange)
)

SELECT
    check_hour,
    total_rows_inserted,
    n_queries,
    minutes_ago,
    CASE 
        -- Healthy: We have rows being inserted
        WHEN total_rows_inserted > 0 THEN 'HEALTHY'
        
        -- Expected Lag: No rows yet, but the hour is too recent (within last 3 hours)
        WHEN total_rows_inserted = 0 AND minutes_ago <= 180 THEN 'EXPECTED_LAG'
        
        -- Gap: No rows inserted for more than 3 hours
        WHEN total_rows_inserted = 0 AND minutes_ago > 180 THEN 'GAP_DETECTED'
        
        -- Silent Failure: Queries ran but 0 rows were inserted (Empty loads)
        WHEN total_rows_inserted = 0 AND n_queries > 0 THEN 'SILENT_FAILURE'
        
        ELSE 'UNKNOWN'
    END AS status
FROM joined