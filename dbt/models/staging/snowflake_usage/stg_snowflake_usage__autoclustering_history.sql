/* Staging: Auto-Clustering History.
   Captures credits consumed by the background process of reorganizing 
   table data for optimization.
*/

WITH source AS (
    SELECT * FROM {{ source('snowflake_usage', 'automatic_clustering_history') }}
),

renamed AS (
    SELECT
        table_id,
        table_name,
        schema_name,
        database_name,
        start_time,
        end_time,
        -- Credits consumed by the serverless process
        credits_used,
        -- Volume of data processed during the operation
        num_bytes_reclustered,
        num_rows_reclustered
    FROM source
    -- We filter on the last 30 days to keep the model performant
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)

SELECT * FROM renamed