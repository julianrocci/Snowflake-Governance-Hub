Event tables are all stored in SNOWFLAKE.TELEMETRY.EVENTS on the company account only.

Params :

LOG_LEVEL : My own logs in my ingestion scripts
-- Enable detailed logging for your Stored Procedures / Snowpark Code
ALTER ACCOUNT SET LOG_LEVEL = 'DEBUG';

LOG_EVENT_LEVEL : Infra logs ( DT , Pipes , alerts)
-- Restrict platform logs to critical infrastructure failures only (Dynamic Tables, Snowpipe)
ALTER ACCOUNT SET LOG_EVENT_LEVEL = 'ERROR';

TRACE_LEVEL : Performance Spans durations mainly
-- Enable performance tracing only when specific exceptions or events occur
ALTER ACCOUNT SET TRACE_LEVEL = 'ON_EVENT';



ALTER ACCOUNT SET LOG_EVENT_LEVEL = 'ERROR';