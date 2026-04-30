/*
    Staging: Table Write Activity from Access History.
    Flattens OBJECTS_MODIFIED to get which tables received writes, and when.
    Used for freshness monitoring per table.
*/

WITH raw_writes AS (
    SELECT
        ah.query_id,
        ah.query_start_time,
        om.value:objectName::STRING AS full_table_name
    FROM {{ source('snowflake_usage', 'access_history') }} ah,
        LATERAL FLATTEN(input => ah.objects_modified) om
    WHERE om.value:objectDomain::STRING = 'Table'
      AND ah.query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)

SELECT
    query_id,
    query_start_time,
    full_table_name,
    SPLIT_PART(full_table_name, '.', 1) AS database_name,
    SPLIT_PART(full_table_name, '.', 2) AS schema_name,
    SPLIT_PART(full_table_name, '.', 3) AS table_name
FROM raw_writes