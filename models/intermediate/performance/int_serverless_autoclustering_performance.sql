/* Intermediate: Serverless Auto-Clustering Performance.
   Calculates daily clustering costs and volumes per table.
   Aggregated by day to allow both daily trend analysis and monthly rollups in Marts.
*/

{{ config(
    materialized='ephemeral'
) }}

WITH base_clustering AS (
    SELECT
        -- Identifying the table precisely
        database_name || '.' || schema_name || '.' || table_name AS full_table_name,
        database_name,
        schema_name,
        table_name,
        -- Truncating to day and month for better reporting
        DATE_TRUNC('day', start_time) AS event_date,
        DATE_TRUNC('month', start_time) AS event_month,
        credits_used,
        num_bytes_reclustered
    FROM {{ ref('stg_autoclustering_history') }}
),

daily_aggregation AS (
    SELECT
        full_table_name,
        database_name,
        schema_name,
        table_name,
        event_date,
        event_month,
        SUM(credits_used) AS daily_credits_used,
        ROUND(SUM(num_bytes_reclustered) / POWER(1024, 3), 2) AS daily_gb_reclustered
    FROM base_clustering
    GROUP BY
        full_table_name,
        database_name,
        schema_name,
        table_name,
        event_date,
        event_month
)

SELECT * FROM daily_aggregation