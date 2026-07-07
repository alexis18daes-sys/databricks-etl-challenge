# AI Usage

## Tools used

- **Claude Code** (Anthropic CLI agent), used as a pair-programming assistant
  with shell/file access throughout this project.

## Division of work

- **Mine**: requirements and scope (read `CHALLENGE.md`, decided what the
  pipeline needed to do), the environment/architecture decisions (running
  against a real local Spark + Delta Lake setup instead of unexecuted code,
  where the repo lives, when to `git init`/commit/push), and reviewing the
  output at each step (bronze/silver/gold row counts, the rejection reasons,
  the two Gold tables, the idempotency reruns) before accepting it.
- **AI-assisted**: the actual implementation — the Bronze/Silver/Gold module
  code (`src/`), the data-quality rule logic and rejection-reason labels
  (`src/silver/`), the two Gold SQL queries and the
  validation/reconciliation/Delta-audit SQL (`sql/`), the pytest suite,
  `azure-pipelines.yml`, and the first draft of the README/this file.

I'm disclosing this level of AI involvement explicitly, per the challenge's
instructions, rather than understating it — and I've since gone through the
pipeline manually end to end (commands and expected output below) so I can
walk through and defend every design decision in it.

## What was manually validated

Every layer was actually run against `data/`, not just read for
plausibility: row counts were checked after Bronze, after Silver (including
the 11 specific rejected records and their reasons, and that duplicate
employee `1008` resolves to the later "Daniel Wilson Updated" record), and
after Gold (both metrics' numbers cross-checked against a manual read of the
source CSVs/JSON). Idempotency was verified by running the pipeline twice in
a row and confirming row counts didn't change and `DESCRIBE HISTORY` shows
exactly one new `MERGE` version per rerun. The full `pytest` suite was run
locally, not just written.

Running everything for real (rather than trusting a clean-looking diff)
caught two bugs that would otherwise have shipped silently:

- The API JSON reader was missing `multiLine=True`. Spark's default JSON
  reader treats each *line* as a record; without this option all 8 API
  records silently disappeared (bronze count was 23 instead of the correct
  31 — no exception, just quietly wrong data).
- On Windows, PySpark's executor Python workers were resolving `python` to
  the Microsoft Store alias stub instead of the real interpreter, failing
  every join/shuffle with `Python worker failed to connect back`. Fixed by
  pinning `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to `sys.executable`
  (Windows-local issue only, irrelevant on Databricks).

## AI suggestions rejected or corrected

- The first version of the Gold SQL used `CREATE OR REPLACE TABLE ... AS
  SELECT`, which works on Databricks/Unity Catalog but throws
  `UNSUPPORTED_FEATURE.TABLE_OPERATION` against the local Hive-metastore +
  `DeltaCatalog` setup used for local execution. Replaced with the more
  portable `DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT`, verified in
  both environments.
- The `javax.jdo.option.ConnectionURL` Spark config (for the local Derby
  metastore to persist across separate process runs) was initially passed
  without the required `spark.hadoop.` prefix and got silently ignored;
  corrected to the properly-namespaced key instead of left relying on it
  happening to work anyway.

## Risks of using AI for data engineering work

- **Plausible-looking silent data loss.** The `multiLine` bug is the clear
  example: the code ran without error and simply returned 0 rows where 8
  were expected. AI-generated ETL code needs row counts checked against an
  independent expectation, not just "it ran without an exception."
- **Confident claims of "done" without execution.** It's easy for generated
  code to look correct and never be run end-to-end against real data. The
  mitigation here was insisting on a real local Spark/Delta environment and
  running every layer before moving on.
- **Environment-specific fixes leaking into "the solution."** The Windows
  `PYSPARK_PYTHON` fix and the `snappy` codec override are both artifacts of
  developing on Windows without Databricks access — harmless there, but
  worth knowing they aren't Databricks best practice, just local workarounds.
- **Assumptions stated with unwarranted confidence.** Business-rule calls
  (the Metric 2 average denominator, the duplicate tie-breaker) are genuine
  judgment calls where reasonable people could disagree; they're called out
  explicitly in the README instead of presented as the only correct reading.
