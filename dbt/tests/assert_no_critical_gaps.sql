-- This test fails if any hour is flagged as a gap
-- A gap is defined as < 10 queries after 3 hours of expected latency
SELECT *
FROM {{ ref('int_monitoring_freshness_gaps') }}
WHERE status = 'GAP_DETECTED'