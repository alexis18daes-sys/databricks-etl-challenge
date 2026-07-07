-- Delta Lake transaction history for the idempotent, mergeable table.
-- Each pipeline run should add at most one new version per table (a MERGE
-- operation), never a version per source row.
DESCRIBE HISTORY silver.hired_employees;

-- Time travel: read the table as it looked right after the very first load.
SELECT COUNT(*) AS row_count_at_version_0
FROM silver.hired_employees VERSION AS OF 0;

-- Change Data Feed (optional). silver.hired_employees is created with
-- delta.enableChangeDataFeed = true (see src/common/tables.py), so all
-- row-level inserts/updates/deletes since version 0 can be inspected here -
-- e.g. the correction to employee 1008 ("Daniel Wilson" -> "Daniel Wilson
-- Updated") shows up as a pair of pre/post-image rows.
SELECT *
FROM table_changes('silver.hired_employees', 0)
ORDER BY _commit_version, id;
