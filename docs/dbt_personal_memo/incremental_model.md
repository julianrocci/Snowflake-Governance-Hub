STRATEGY :

append :
simply add every new column, no deduplication. Every new row is saved

merge: Default strategy, insert new rows and update rows based on unique_key

delete+insert: delete rows based on unique_key and reinsert with new data, insert new rows.

insert+overwrite: overwrite data per micro partition

microbatch: handle batches based a date, periodically run batches by splitting them by partition. good to handle late arriving data.


None of the strategy handles delete by default, if you need to delete rows ; the best way is to use post hook in your model.


Best optimized model:
If your sources can send you an updated_at field, you can add an ingested_at column in your landing table ->

----------------------
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'customer_id',
    
    -- The post_hook runs AFTER the merge, but uses the last_ingested_at captured BEFORE the merge
    post_hook = [
      "DELETE FROM {{ this }} WHERE customer_id IN (
          SELECT customer_id FROM {{ ref('landing_customers') }} WHERE _fivetran_deleted = TRUE
            -- Clean up only the hard deletes that arrived in the current batch
            AND ingested_at > '{{ last_ingested_at }}'
      )"
    ]
  )
}}
{% if is_incremental() %}
    -- Create the SQL query string using string
    {% set query = "SELECT MAX(ingested_at) FROM " ~ this %}
    
    -- Execute the query and retrieve the single value [row 0][col 0]
    {% set last_ingested_at = run_query(query).columns[0][0] %}
{% else %}
    -- Fallback date for the very first run (Full Refresh) when the target table doesn't exist yet
    {% set last_ingested_at = '1970-01-01' %}
{% endif %}

-- MAIN INCREMENTAL MODEL
SELECT 
    customer_id,
    name,
    ingested_at
FROM {{ ref('landing_customers') }}

WHERE 
  -- Exclude soft-deleted records from the main merge payload so they don't overwrite active rows
  NOT _fivetran_deleted 

  -- Filter for the incremental delta using our frozen timestamp variable (works for both incremental and full refresh)
  AND ingested_at > '{{ last_ingested_at }}'
----------------------
This model process perfectly only the delta and also handles late arriving data perfectly