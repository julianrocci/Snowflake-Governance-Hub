-- SILVER LAYER: Joins & Deduplication
CREATE OR REPLACE DYNAMIC TABLE analytics.prod.dt_silver_users
  TARGET_LAG = '30 minutes'
  WAREHOUSE = xsmall_wh
  INITIALIZATION_WAREHOUSE = xlarge_wh -- Big compute used ONLY for the first full scan
  REFRESH_MODE = INCREMENTAL
AS
SELECT * EXCLUDE (_fivetran_synced, _fivetran_deleted, _snowflake_loaded_at)
FROM raw_db.bronze.raw_users
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- GOLD LAYER: Aggregations
CREATE OR REPLACE DYNAMIC TABLE analytics.prod.dt_gold_user_metrics
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = xsmall_wh
  INITIALIZATION_WAREHOUSE = xlarge_wh
  REFRESH_MODE = INCREMENTAL
AS
SELECT country, COUNT(DISTINCT id) AS active_users
FROM analytics.prod.dt_silver_users
GROUP BY country;


How to handle backfilled data :

If you have a small volume of the table to insert/update use UPSERT method (doesn't modify Snowflake's change tracking ).
Otherwise for big loads use INSERT OVERWRITE method which force you to do a full REFRESH MODE.

ALTER DYNAMIC TABLE analytics.prod.dt_silver_users REFRESH MODE = FULL MANDATORY;


The PRIMARY KEY RELY keyword: 

-- Enforce the trust flag on the parent/base table
ALTER TABLE analytics.prod.dt_silver_users ADD PRIMARY KEY (id) RELY;

The Problem: An INSERT OVERWRITE or full-history reload on a base table resets Snowflake's physical change tracking metadata. Without the RELY, this triggers an accidental high-cost Full Refresh cascade on all downstream tables.

The Fix: Declare a PRIMARY KEY (...) RELY on the parent table. It tells Snowflake: "Trust this unique key for change detection if physical tracking logs are wiped."

The Benefit: Downstream Dynamic Tables stay INCREMENTAL. They automatically compute the delta via join-based detection, processing only modified/new records instead of scanning billions of rows from scratch.

BI Optimization: It also allows Snowflake to perform Join Elimination on LEFT JOIN queries when no columns from the dimension are requested.