# Hiring Data ETL Pipeline (Bronze / Silver / Gold)

A small PySpark + Delta Lake pipeline that lands daily CSV batches and a
paginated mock API, applies data-quality rules, loads clean Delta tables
idempotently, and produces two Gold analytical outputs — built for the
Databricks ETL take-home challenge (see [`CHALLENGE.md`](CHALLENGE.md)).

The code is plain PySpark/SparkSQL with no Databricks-only APIs, so it runs
identically on a Databricks cluster or on a laptop; only the storage paths
change (see [Running in Azure Databricks](#running-in-azure-databricks)).

## Project layout

```text
src/
  common/       spark session factory, paths, schemas, Delta/table helpers
  bronze/       CSV + JSON API extraction -> bronze.*_raw
  silver/       data-quality rules, dedup, rejected_records, idempotent load
  gold/         runs the SparkSQL in sql/ to build the two Gold tables
sql/            the actual Gold table SQL + validation/reconciliation/audit queries
notebooks/      thin Databricks-notebook wrappers around src/
scripts/        local entrypoints (run_pipeline, run_validation, setup_local_env)
tests/          pytest unit tests (local Spark + Delta)
data/           the provided landing/ CSVs and api/ JSON pages
azure-pipelines.yml
AI_USAGE.md
```

## How to run it

### 1. Local (no Databricks access)

Requirements: Python 3.11. That's it — `scripts/setup_local_env.py` checks
for a working `java` on `JAVA_HOME`/`PATH` and only downloads a portable
JDK 17 if none is found (it never touches or replaces one you already
have), plus the Delta Lake jars and, on Windows, the Hadoop `winutils.exe`
shim (Delta/Spark's local filesystem calls fail on Windows without it).

```bash
pip install -r requirements-dev.txt
python scripts/setup_local_env.py     # downloads whatever's missing (JDK/jars/winutils) into ~/tools
# then export JAVA_HOME / HADOOP_HOME / PATH as printed by the script

python scripts/run_pipeline.py        # Bronze -> Silver -> Gold, end to end
python scripts/run_validation.py      # prints reconciliation + Delta audit queries
python -m pytest tests/ -v            # unit tests
```

Why the extra setup: PySpark needs a JVM plus the Delta Lake jars. On
Databricks these are already on the cluster; locally we pin exact jar
versions (matching the `delta-spark` pip package) instead of letting Ivy
resolve them at runtime, since dependency resolution over Maven Central
isn't guaranteed to work through every network. `src/common/spark_session.py`
also enables Hive support with a Derby metastore under `_lakehouse/`, so the
`bronze`/`silver`/`gold` schemas persist across separate script runs, not
just within one Spark session — otherwise every `python scripts/...`
invocation would start from an empty catalog.

Everything lands under `_lakehouse/` (gitignored); delete that folder to
reset to a clean slate.

### 2. Azure Databricks

See [Running in Azure Databricks](#running-in-azure-databricks) below.

## Main design decisions

- **Medallion architecture** (Bronze -> Silver -> Gold) with each layer as
  real Delta tables registered in a metastore (`CREATE SCHEMA` /
  `CREATE TABLE ... USING DELTA LOCATION '...'`), not just files — so
  `spark.sql("SELECT * FROM silver.hired_employees")` works exactly like it
  would in a notebook.
- **Explicit schemas everywhere** (`src/common/schemas.py`) instead of
  `inferSchema`, so a malformed CSV/JSON value becomes a typed `NULL` deterministically
  rather than silently changing the inferred column type.
- **Rules and I/O are separated**: `src/silver/transform.py` /
  `quality_rules.py` are pure DataFrame-in/DataFrame-out functions with no
  reads or writes, so they're unit-testable with small in-memory DataFrames
  (see `tests/`) without touching Delta or the metastore.
- **Nothing is silently dropped.** Every Bronze row ends up in exactly one
  place: the clean Silver table, or `silver.rejected_records` with a specific
  reason. The reconciliation query in `sql/validation_queries.sql` asserts
  `bronze_count == silver_valid_count + rejected_count`.

## Assumptions

- The mock API's `data` array and the CSV rows describe the same
  `hired_employees` entity, so both land in one `bronze.hired_employees_raw`
  table (each row keeps its own `source_system`/`source_file`).
- Datetimes are ISO-8601 with a literal trailing `Z` (as in the sample data,
  e.g. `2021-01-15T10:00:00Z`); parsed with `try_to_timestamp` so a
  malformed value becomes `NULL` (rejected) instead of failing the job.
- Metric 2's "average number of employees hired by department" is computed
  only over departments with at least one 2021 hire (`AVG` over the
  per-department counts) — departments with zero 2021 hires are excluded
  from both the average and the output. This is documented in
  `sql/gold_metric_2_departments_above_average.sql`.
- A department/job reference is only valid if that department/job **itself**
  passed Silver's own validation — `hired_employees` is validated against
  `silver.departments`/`silver.jobs`, not the raw Bronze dimensions.

## Data quality rules (Silver)

Common to all three entities: `id` must be non-null (schema casting already
turns a non-integer value into `NULL`, so "must be a valid integer" and
"must not be null" collapse into one check), and duplicate `id`s are
resolved deterministically (see [Load strategy](#load-strategy-and-idempotency)).

| Entity | Rule | Rejection reason label |
|---|---|---|
| departments / jobs | `id` not null | `id_is_null` |
| departments / jobs | name/job not null or empty (after trim) | `department_is_null_or_empty` / `job_is_null_or_empty` |
| hired_employees | `name` not null/empty | `name_is_null_or_empty` |
| hired_employees | `datetime` present | `datetime_is_null` |
| hired_employees | `datetime` parses as ISO timestamp | `datetime_invalid_iso_format` |
| hired_employees | `department_id` present | `department_id_is_null` |
| hired_employees | `department_id` exists in `silver.departments` | `department_id_not_found_in_departments` |
| hired_employees | `job_id` present | `job_id_is_null` |
| hired_employees | `job_id` exists in `silver.jobs` | `job_id_not_found_in_jobs` |
| all | duplicate id, older copy | `duplicate_id_superseded_by_newer_record` |

A row can fail more than one rule at once; `silver/quality_rules.reasons_col`
collects every failed label into the single `rejection_reason` column
(`; `-joined) instead of only reporting the first match.

## Load strategy and idempotency

`silver.departments`, `silver.jobs`, `silver.hired_employees` and
`silver.rejected_records` are all loaded with **`MERGE INTO`**
(`src/common/tables.merge_into_table`), keyed by `id` (and by
`(table_name, record_id, source_file)` for `rejected_records`).
Re-running the pipeline against unchanged input therefore updates existing
rows in place and inserts nothing new — no duplicates, confirmed by the
duplicate-check query and by running `scripts/run_pipeline.py` twice locally.

`bronze.*_raw` uses a different, cheaper idempotent strategy since it's raw
landing data with no business rules: **selective overwrite by
`source_file`** (Delta `replaceWhere`). Re-ingesting the same file overwrites
just that file's rows; a new file is simply added. `MERGE INTO` would work
here too, but replaceWhere avoids a row-by-row merge scan for what is really
a batch of complete-file replacements.

**Duplicate tie-breaker** (documented as requested): keep the row with the
latest `source_batch_date` — the date embedded in the source file name
(e.g. `hired_employees_2021_01_02.csv` -> `2021-01-02`), since that's the
actual business date of the batch. Falls back to `ingestion_date` for
sources without a dated file name (the API pages), and breaks further ties
with `ingestion_timestamp`. See `src/silver/quality_rules.py`. This is why
employee `1008` ("Daniel Wilson" in the `01_01` file, "Daniel Wilson
Updated" in `01_02`) resolves to the `01_02` version.

## Rejected / discarded records

Nothing is dropped silently. Every row that fails a Silver rule, plus every
row that loses a duplicate-id tie-break, is written to
`silver.rejected_records` (also loaded via `MERGE INTO`, so reruns don't
duplicate rejections either) with the original row preserved as JSON in
`raw_payload` and a specific `rejection_reason` — see
[Data quality rules](#data-quality-rules-silver) for the full list of labels
this pipeline currently produces on the sample data.

## Gold layer

Built from plain SparkSQL files in `sql/`, executed by `src/gold/build_gold.py`:

- `gold.hires_by_department_job_quarter_2021` — hires per
  department/job/quarter in 2021, `department ASC, job ASC`.
- `gold.departments_above_average_hiring_2021` — departments whose 2021
  hires exceed the average per-department hire count, `hired DESC`.

Both files use `DROP TABLE IF EXISTS` + `CREATE TABLE ... AS SELECT` rather
than `CREATE OR REPLACE TABLE AS SELECT`: the latter isn't supported by the
local Hive-metastore + `DeltaCatalog` combination used for local execution in
this repo (it works fine directly on Databricks/Unity Catalog, but this
keeps one code path for both). Delta doesn't guarantee row order is
persisted on disk, so `ORDER BY` in the CTAS is for readability of the
definition — always re-`ORDER BY` when querying, as the validation/notebook
queries do.

## Validation and reconciliation

`sql/validation_queries.sql` — row counts per layer, the
Bronze-equals-Silver-plus-rejected reconciliation, a rejection-reason
breakdown, the duplicate check, and a referential-integrity check.
`sql/delta_audit_queries.sql` — `DESCRIBE HISTORY`, `VERSION AS OF 0`
time travel, and an optional Change Data Feed query (`silver.hired_employees`
is created with `delta.enableChangeDataFeed = true`). Run both via
`python scripts/run_validation.py`, or as `%sql` cells in
`notebooks/01_validation_and_audit.py` on Databricks.

On the provided sample data: **31** Bronze rows -> **20** valid +
**11** rejected (2 null names, 2 invalid datetimes, 2 null department_ids, 1
unknown department_id, 2 null job_ids, 1 unknown job_id, 1 superseded
duplicate) = 31. Zero duplicate ids, zero referential-integrity violations.

## Running in Azure Databricks

No code changes are required, only configuration:

1. Import this repo as a **Databricks Repo** (or build/install a wheel from
   `src/`) so `import src...` resolves on the cluster.
2. Upload `data/landing` and `data/api` to a Unity Catalog **Volume** (or a
   DBFS mount), e.g. `/Volumes/main/hiring/raw/landing`.
3. Set `ETL_DATA_ROOT` and `ETL_LAKEHOUSE_ROOT` (env vars, or the widgets in
   `notebooks/00_run_pipeline.py`) to that Volume path and to where you want
   the Delta tables stored, e.g. `/Volumes/main/hiring/lakehouse`.
4. Run `notebooks/00_run_pipeline.py` then `notebooks/01_validation_and_audit.py`
   (or the equivalent Databricks Job with those two notebooks as tasks).

`src/common/spark_session.get_spark()` detects `DATABRICKS_RUNTIME_VERSION`
and, on a Databricks cluster, just returns the cluster's already-configured
session (Delta + Unity Catalog metastore are pre-wired there) — none of the
local jar/metastore/winutils plumbing in this README applies.

## What I'd improve before production

- Partition `silver.hired_employees` / Bronze tables by ingestion or hire
  date, and add `OPTIMIZE` / `ZORDER` (or auto-compaction) once data volume
  is non-trivial — irrelevant at this dataset's size but not at real scale.
- Replace the ad-hoc `source_file` date-regex tie-breaker with an explicit
  `batch_date` column supplied by the upstream system, rather than parsed
  out of a filename convention.
- Add schema-level constraints (`CHECK` constraints, `NOT NULL` at the
  Delta table level for `id`) so a bug in the Python validation code can't
  silently regress data quality.
- Wire real alerting on the reconciliation query (e.g. a Databricks SQL
  alert if `bronze_count != silver_valid + rejected`) instead of a
  manually-run script.
- Restore the default `snappy` Parquet codec once off Windows — it's only
  overridden locally because of a native-library loading issue specific to
  this dev machine (see comment in `src/common/spark_session.py`); Databricks
  is unaffected.
