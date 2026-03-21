Use Cases
Credit Monitoring: Notify administrators if a specific warehouse exceeds a daily credit quota.

Data Integrity: Detect if a pipeline has loaded 0 rows in the last 4 hours.

Security: Alert on multiple failed login attempts within a short timeframe.

Data Spilling: Identify queries spilling to disk (Local/Remote) to trigger warehouse resizing or SQL tuning.

Long Running Queries: Monitor and catch "runaway queries" exceeding expected execution time limits.

Performance : Identify the clustering depth of a table and alert with threshold

Business Logic: Notify the sales team if a single transaction exceeds a certain amount.

Should use a dedicated WH for Alerts