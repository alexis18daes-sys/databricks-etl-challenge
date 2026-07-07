"""Bronze -> Silver validation rules for departments, jobs and hired_employees."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.silver.quality_rules import (
    reasons_col,
    split_duplicates,
    to_rejected_records,
    with_source_batch_date,
)


def transform_dimension(bronze_df: DataFrame, table_name: str, name_col: str) -> "tuple[DataFrame, DataFrame]":
    """Shared validation for the small reference dimensions (departments, jobs):
    id and name/job must not be null/empty; duplicate ids keep the latest row
    (see quality_rules.split_duplicates for the tie-breaker).
    """
    df = with_source_batch_date(bronze_df)

    df = df.withColumn(
        "_reasons",
        reasons_col(
            (F.col("id").isNull(), "id_is_null"),
            (F.col(name_col).isNull() | (F.trim(F.col(name_col)) == ""), f"{name_col}_is_null_or_empty"),
        ),
    )

    invalid_format = df.filter(F.size("_reasons") > 0)
    valid_format = df.filter(F.size("_reasons") == 0)

    kept, superseded = split_duplicates(valid_format, "id")
    superseded = superseded.withColumn("_reasons", F.array(F.lit("duplicate_id_superseded_by_newer_record")))

    payload_cols = ["id", name_col, "source_system", "source_file", "ingestion_timestamp", "ingestion_date", "batch_id"]
    rejected = to_rejected_records(
        invalid_format.unionByName(superseded),
        table_name=table_name,
        id_col="id",
        payload_cols=payload_cols,
    )

    clean = kept.select("id", name_col)
    return clean, rejected


def transform_hired_employees(
    bronze_df: DataFrame, silver_departments: DataFrame, silver_jobs: DataFrame
) -> "tuple[DataFrame, DataFrame]":
    """Validation for hired_employees, including referential integrity
    against the (already validated) silver.departments / silver.jobs
    dimensions.
    """
    df = with_source_batch_date(bronze_df)
    df = df.withColumn(
        "hire_datetime",
        F.expr("try_to_timestamp(datetime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
    )

    dept_ids = silver_departments.select(F.col("id").alias("_dept_id"))
    job_ids = silver_jobs.select(F.col("id").alias("_job_id"))
    df = df.join(dept_ids, df.department_id == dept_ids._dept_id, "left").join(
        job_ids, df.job_id == job_ids._job_id, "left"
    )

    df = df.withColumn(
        "_reasons",
        reasons_col(
            (F.col("id").isNull(), "id_is_null"),
            (F.col("name").isNull() | (F.trim(F.col("name")) == ""), "name_is_null_or_empty"),
            (F.col("datetime").isNull(), "datetime_is_null"),
            (F.col("datetime").isNotNull() & F.col("hire_datetime").isNull(), "datetime_invalid_iso_format"),
            (F.col("department_id").isNull(), "department_id_is_null"),
            (
                F.col("department_id").isNotNull() & F.col("_dept_id").isNull(),
                "department_id_not_found_in_departments",
            ),
            (F.col("job_id").isNull(), "job_id_is_null"),
            (F.col("job_id").isNotNull() & F.col("_job_id").isNull(), "job_id_not_found_in_jobs"),
        ),
    ).drop("_dept_id", "_job_id")

    invalid_format = df.filter(F.size("_reasons") > 0)
    valid_format = df.filter(F.size("_reasons") == 0)

    kept, superseded = split_duplicates(valid_format, "id")
    superseded = superseded.withColumn("_reasons", F.array(F.lit("duplicate_id_superseded_by_newer_record")))

    payload_cols = [
        "id",
        "name",
        "datetime",
        "department_id",
        "job_id",
        "source_system",
        "source_file",
        "ingestion_timestamp",
        "ingestion_date",
        "batch_id",
    ]
    rejected = to_rejected_records(
        invalid_format.unionByName(superseded),
        table_name="hired_employees",
        id_col="id",
        payload_cols=payload_cols,
    )

    clean = kept.select("id", "name", "hire_datetime", "department_id", "job_id")
    return clean, rejected
