"""Extract landing CSV batches and mock API JSON pages into Bronze Delta tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.paths import api_path, landing_path
from src.common.schemas import (
    DEPARTMENTS_SCHEMA,
    HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA,
    HIRED_EMPLOYEES_SCHEMA,
    JOBS_SCHEMA,
)
from src.common.tables import write_bronze_table

_HIRED_EMPLOYEES_COLUMNS = ["id", "name", "datetime", "department_id", "job_id", "source_system", "source_file"]


def _basename_col():
    return F.element_at(F.split(F.input_file_name(), "/"), -1)


def _add_bronze_metadata(df: DataFrame, batch_id: str, source_system: str = None) -> DataFrame:
    """Add the technical metadata columns required for every bronze table."""
    if "source_system" not in df.columns:
        df = df.withColumn("source_system", F.lit(source_system))
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("batch_id", F.lit(batch_id))
    )


def ingest_departments(spark: SparkSession, batch_id: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .schema(DEPARTMENTS_SCHEMA)
        .csv(landing_path("departments", "*.csv"))
        .withColumn("source_file", _basename_col())
    )
    df = _add_bronze_metadata(df, batch_id, source_system="landing_csv")
    write_bronze_table(spark, df, "departments_raw")
    return df


def ingest_jobs(spark: SparkSession, batch_id: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .schema(JOBS_SCHEMA)
        .csv(landing_path("jobs", "*.csv"))
        .withColumn("source_file", _basename_col())
    )
    df = _add_bronze_metadata(df, batch_id, source_system="landing_csv")
    write_bronze_table(spark, df, "jobs_raw")
    return df


def _read_hired_employees_csv(spark: SparkSession) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .schema(HIRED_EMPLOYEES_SCHEMA)
        .csv(landing_path("hired_employees", "*.csv"))
        .withColumn("source_system", F.lit("landing_csv"))
        .withColumn("source_file", _basename_col())
    )
    return df.select(*_HIRED_EMPLOYEES_COLUMNS)


def _read_hired_employees_api(spark: SparkSession) -> DataFrame:
    envelope = (
        spark.read.option("multiLine", True)
        .schema(HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA)
        .json(api_path("hired_employees_page_*.json"))
        .withColumn("source_file", _basename_col())
    )
    df = envelope.select(
        F.explode("data").alias("rec"),
        "source_system",
        "source_file",
    ).select("rec.*", "source_system", "source_file")
    return df.select(*_HIRED_EMPLOYEES_COLUMNS)


def ingest_hired_employees(spark: SparkSession, batch_id: str) -> DataFrame:
    """Land both the daily CSV batches and the paginated API responses into a
    single bronze table, since both describe the same `hired_employees`
    source entity."""
    combined = _read_hired_employees_csv(spark).unionByName(_read_hired_employees_api(spark))
    combined = _add_bronze_metadata(combined, batch_id)
    write_bronze_table(spark, combined, "hired_employees_raw")
    return combined
