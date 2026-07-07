"""Shared data-quality helpers used by every Silver transform."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def with_source_batch_date(df: DataFrame) -> DataFrame:
    """Business date used to break ties between duplicate ids.

    Primary tie-breaker: the date embedded in the source file name (e.g.
    `hired_employees_2021_01_02.csv` -> 2021-01-02), i.e. the batch's actual
    business date. Falls back to the ingestion date for sources without a
    dated file name (the mock API pages), and as a secondary tie-breaker
    when two files share the same embedded date.
    """
    return df.withColumn(
        "source_batch_date",
        F.coalesce(
            F.to_date(F.regexp_extract("source_file", r"(\d{4}_\d{2}_\d{2})", 1), "yyyy_MM_dd"),
            F.to_date("ingestion_timestamp"),
        ),
    )


def reasons_col(*conditions_and_labels) -> "F.Column":
    """Build an array<string> column listing every failed rule's label."""
    exprs = [F.when(cond, F.lit(label)) for cond, label in conditions_and_labels]
    return F.filter(F.array(*exprs), lambda x: x.isNotNull())


def split_duplicates(df: DataFrame, id_col: str) -> "tuple[DataFrame, DataFrame]":
    """Deterministic dedup: keep the row with the latest source_batch_date,
    breaking further ties with the latest ingestion_timestamp. Everything
    else sharing the same id is "superseded" and returned separately so it
    stays traceable in silver.rejected_records instead of being silently
    dropped.
    """
    window = Window.partitionBy(id_col).orderBy(F.col("source_batch_date").desc(), F.col("ingestion_timestamp").desc())
    ranked = df.withColumn("_rn", F.row_number().over(window))
    kept = ranked.filter(F.col("_rn") == 1).drop("_rn")
    superseded = ranked.filter(F.col("_rn") > 1).drop("_rn")
    return kept, superseded


def to_rejected_records(df: DataFrame, table_name: str, id_col: str, payload_cols: list) -> DataFrame:
    """Project a dataframe (carrying a `_reasons` array<string> column) into
    the standard `silver.rejected_records` shape."""
    return df.select(
        F.lit(table_name).alias("table_name"),
        F.col(id_col).cast("string").alias("record_id"),
        F.col("source_system").alias("source_system"),
        F.col("source_file").alias("source_file"),
        F.to_json(F.struct(*payload_cols)).alias("raw_payload"),
        F.array_join("_reasons", "; ").alias("rejection_reason"),
        F.current_timestamp().alias("rejection_timestamp"),
        F.col("batch_id").alias("batch_id"),
    )
