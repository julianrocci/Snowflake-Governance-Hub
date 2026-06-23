Backfilling in Near Real-Time Environments :

1 Strategy : Partitioned Dynamic Overwrite (The Insert Overwrite Approach)
    Instead of replacing the whole table, you replace only the specific parts of time that need backfilling.

Logic:
    Load backfill data into a temporary staging table.
    Use a transaction to delete the specific time range in Prod and insert from Stage.

Pros: Only affects specific time ranges; production stays live for other periods.

"Cons": Requires a clear time-based clustering key (event_timestamp for example)


2 Strategy : The Idempotent Merge (Upsert Approach)
    You treat the backfill data exactly like incoming real-time data.

Logic: 
    Use the MERGE statement. If the record exists (backfill matches prod), update it with the new logic. If it doesn't exist, insert it.

Pros: No downtime; handles late-arriving data naturally.

Cons: MERGE is more compute-intensive than INSERT for very large volumes (So more expensive).



Backfilling in (micro-)Batching Environments :

1 Strategy : Partitioned Dynamic Overwrite (The Insert Overwrite Approach) SAME AS Near Real-Time Environments

Example:
-- Delete existing faulty data for April
DELETE FROM sales_prod WHERE sale_date BETWEEN '2026-04-01' AND '2026-04-30';

-- Insert corrected historical data
INSERT INTO sales_prod
SELECT * FROM s3_stage_historical WHERE file_date = '2026-04';


2 Strategy : Zero-Copy Clone & Swap 
Create a sandbox environment from production, perform the backfill, and swap once validated.

Pros: Zero risk to production; no downtime; instant rollback capability.

Cons: Requires additional storage for the changes made during backfill.
Example : 
-- 1. Clone production table
CREATE TABLE sales_backfill_clone CLONE sales_prod;

-- 2. Run backfill logic on the clone
INSERT INTO sales_backfill_clone
SELECT * FROM legacy_source_system 
WHERE sale_date < '2026-01-01';

-- 3. Swap after validation
ALTER TABLE sales_prod SWAP WITH sales_backfill_clone;