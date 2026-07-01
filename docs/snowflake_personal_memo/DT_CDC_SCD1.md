Example of Dynamic table:

SELECT * EXCLUDE (_fivetran_synced, _fivetran_deleted, _snowflake_loaded_at)
FROM raw_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1


Could be usefull as an intermediate table or mart table as logs events tables where you want to retrieve the last event per user.

The ORDER BY part handles the out-of-order arrival by taking the most recent value based on the updated_at time.

The EXCLUDE part prevents your pipeline to break in case of schema evolution where new columns are added by the source.