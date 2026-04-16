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
    {{ get_domain_from_database('database_name') }} AS domain
FROM monthly_table_aggregation
ORDER BY event_month DESC, total_monthly_credits DESC