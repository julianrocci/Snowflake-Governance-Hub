/* Mart: Serverless Auto-Clustering Mart.
   Aggregates clustering costs by month, domain and table.
   Used to identify high-cost tables and monitor serverless credit consumption.
*/

{{ config(
    materialized='table',
    schema='mart_performance'
) }}

WITH int_clustering AS (
    SELECT * FROM {{ ref('int_serverless_autoclustering_performance') }}
),

monthly_table_aggregation AS (
    SELECT
        event_month,
        full_table_name,
        database_name,
        schema_name,
        table_name,
        SUM(daily_credits_used) AS total_monthly_credits,
        SUM(daily_gb_reclustered) AS total_monthly_gb_reclustered,
        -- Average daily cost
        ROUND(AVG(daily_credits_used), 2) AS avg_daily_credits
    FROM int_clustering
    GROUP BY 
        event_month, 
        full_table_name, 
        database_name, 
        schema_name, 
        table_name
)

SELECT 
    *,
    -- Domain Mapping
    CASE 
        WHEN database_name LIKE 'FIN_%' OR schema_name LIKE 'FIN_%' THEN 'FINANCE'
        WHEN database_name LIKE 'MKT_%' OR schema_name LIKE 'MKT_%' THEN 'MARKETING'
        WHEN database_name LIKE 'ECO_%' OR schema_name LIKE 'ECO_%' THEN 'ECOMMERCE'
        WHEN database_name LIKE 'RET_%' OR schema_name LIKE 'RET_%' THEN 'RETAIL'
        WHEN database_name LIKE 'ANA_%' OR schema_name LIKE 'ANA_%' THEN 'ANALYTICS'
        WHEN database_name LIKE 'RAW%'   OR schema_name LIKE 'STAGING%' THEN 'DATA_ENG'
        ELSE 'OTHER'
    END AS domain
FROM monthly_table_aggregation
ORDER BY event_month DESC, total_monthly_credits DESC