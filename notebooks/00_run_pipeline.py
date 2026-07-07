# Databricks notebook source
# MAGIC %md
# MAGIC # Hiring ETL - Bronze -> Silver -> Gold
# MAGIC
# MAGIC Thin orchestration notebook. All the actual logic lives in the `src/`
# MAGIC package so it can be unit tested outside of Databricks; this notebook
# MAGIC just wires it together against a real cluster/session.
# MAGIC
# MAGIC **Setup**: attach the repo as a Databricks Repo (or `%pip install` a
# MAGIC wheel built from it) so `src` is importable, then optionally set the
# MAGIC widgets below to point at your Unity Catalog Volume / DBFS paths.

# COMMAND ----------
dbutils.widgets.text("data_root", "", "ETL_DATA_ROOT (e.g. /Volumes/main/hiring/landing_and_api)")
dbutils.widgets.text("lakehouse_root", "", "ETL_LAKEHOUSE_ROOT (e.g. /Volumes/main/hiring/lakehouse)")

# COMMAND ----------
import os

data_root = dbutils.widgets.get("data_root")
lakehouse_root = dbutils.widgets.get("lakehouse_root")
if data_root:
    os.environ["ETL_DATA_ROOT"] = data_root
if lakehouse_root:
    os.environ["ETL_LAKEHOUSE_ROOT"] = lakehouse_root

# COMMAND ----------
from src.bronze.run_bronze import run_bronze
from src.gold.build_gold import run_gold
from src.silver.run_silver import run_silver

# COMMAND ----------
# MAGIC %md ## 1. Bronze - extract CSV batches + mock API JSON pages

# COMMAND ----------
batch_id = run_bronze(spark)
print(f"Bronze ingestion complete. batch_id={batch_id}")

# COMMAND ----------
# MAGIC %md ## 2. Silver - validate, deduplicate, load idempotently (MERGE)

# COMMAND ----------
run_silver(spark)
print("Silver transform complete.")

# COMMAND ----------
# MAGIC %md ## 3. Gold - analytical outputs (SparkSQL)

# COMMAND ----------
run_gold(spark)
print("Gold build complete.")
