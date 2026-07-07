"""Shared helpers for creating schemas and writing Delta tables idempotently."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.common.paths import table_path


def ensure_schema(spark: SparkSession, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def _register_external_table(spark: SparkSession, full_table_name: str, location: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{location}'")


def write_bronze_table(spark: SparkSession, df: DataFrame, table_name: str) -> str:
    """Write a bronze table idempotently, keyed by `source_file`.

    Re-running the pipeline against the same source files must not duplicate
    rows. Each distinct `source_file` present in `df` selectively overwrites
    only the rows previously landed from that same file (Delta's
    `replaceWhere`), while other files already in the table are left
    untouched. A brand new file is simply added.
    """
    ensure_schema(spark, "bronze")
    full_table_name = f"bronze.{table_name}"
    location = table_path("bronze", table_name)

    if not DeltaTable.isDeltaTable(spark, location):
        df.write.format("delta").mode("overwrite").save(location)
    else:
        source_files = [row["source_file"] for row in df.select("source_file").distinct().collect()]
        for source_file in source_files:
            subset = df.filter(F.col("source_file") == source_file)
            (
                subset.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"source_file = '{source_file}'")
                .save(location)
            )

    _register_external_table(spark, full_table_name, location)
    return location


def merge_into_table(
    spark: SparkSession,
    df: DataFrame,
    layer: str,
    table_name: str,
    merge_keys: list,
    table_properties: dict | None = None,
) -> str:
    """Idempotent upsert into a Delta table (create schema/table on first run).

    Uses `MERGE INTO` keyed by `merge_keys`: matching rows are fully
    replaced (handles corrected records such as a late name fix) and new
    keys are inserted. Running the pipeline again with unchanged input
    produces the same target state - no duplicate rows.
    """
    ensure_schema(spark, layer)
    full_table_name = f"{layer}.{table_name}"
    location = table_path(layer, table_name)

    if not DeltaTable.isDeltaTable(spark, location):
        writer = df.write.format("delta")
        if table_properties:
            for key, value in table_properties.items():
                writer = writer.option(key, value)
        writer.mode("overwrite").save(location)
    else:
        target = DeltaTable.forPath(spark, location)
        condition = " AND ".join(f"target.{key} = source.{key}" for key in merge_keys)
        (
            target.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    _register_external_table(spark, full_table_name, location)
    return location
