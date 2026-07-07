# Databricks notebook source
# MAGIC %md
# MAGIC # Validation, reconciliation & Delta audit
# MAGIC
# MAGIC Run after `00_run_pipeline` to confirm row counts reconcile and to
# MAGIC inspect Delta Lake's transaction history / time travel / CDF for
# MAGIC `silver.hired_employees`. Mirrors `sql/validation_queries.sql` and
# MAGIC `sql/delta_audit_queries.sql` as runnable `%sql` cells.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'bronze.departments_raw' AS table_name, COUNT(*) AS row_count FROM bronze.departments_raw
# MAGIC UNION ALL SELECT 'bronze.jobs_raw', COUNT(*) FROM bronze.jobs_raw
# MAGIC UNION ALL SELECT 'bronze.hired_employees_raw', COUNT(*) FROM bronze.hired_employees_raw
# MAGIC UNION ALL SELECT 'silver.departments', COUNT(*) FROM silver.departments
# MAGIC UNION ALL SELECT 'silver.jobs', COUNT(*) FROM silver.jobs
# MAGIC UNION ALL SELECT 'silver.hired_employees', COUNT(*) FROM silver.hired_employees
# MAGIC UNION ALL SELECT 'silver.rejected_records', COUNT(*) FROM silver.rejected_records
# MAGIC UNION ALL SELECT 'gold.hires_by_department_job_quarter_2021', COUNT(*) FROM gold.hires_by_department_job_quarter_2021
# MAGIC UNION ALL SELECT 'gold.departments_above_average_hiring_2021', COUNT(*) FROM gold.departments_above_average_hiring_2021;

# COMMAND ----------
# MAGIC %md ### Reconciliation: bronze = silver valid + rejected

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bronze.hired_employees_raw) AS bronze_raw_count,
# MAGIC   (SELECT COUNT(*) FROM silver.hired_employees) AS silver_valid_count,
# MAGIC   (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'hired_employees') AS rejected_count,
# MAGIC   (SELECT COUNT(*) FROM silver.hired_employees)
# MAGIC     + (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'hired_employees') AS reconciled_total;

# COMMAND ----------
# MAGIC %md ### Duplicate check (must be empty)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT id, COUNT(*) AS n FROM silver.hired_employees GROUP BY id HAVING COUNT(*) > 1;

# COMMAND ----------
# MAGIC %md ### Delta audit: history, time travel, change data feed

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.hired_employees;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count_at_version_0 FROM silver.hired_employees VERSION AS OF 0;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM table_changes('silver.hired_employees', 0) ORDER BY _commit_version, id;
