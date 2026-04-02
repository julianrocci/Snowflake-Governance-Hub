-- Extracts governance dimensions (team, workload, environment)
-- from warehouse-level tags to support cost attribution.

SELECT
    object_name AS warehouse_name,

    MAX(CASE WHEN tag_name = 'TEAM' THEN tag_value END) AS team,
    MAX(CASE WHEN tag_name = 'WORKLOAD' THEN tag_value END) AS workload,
    MAX(CASE WHEN tag_name = 'ENVIRONMENT' THEN tag_value END) AS environment

FROM snowflake.account_usage.tag_references
WHERE object_domain = 'WAREHOUSE'
  AND tag_name IN ('TEAM', 'WORKLOAD', 'ENVIRONMENT')

GROUP BY 1