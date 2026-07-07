"""Orchestrates the Bronze -> Silver validation and idempotent load step."""

from pyspark.sql import SparkSession

from src.common.tables import merge_into_table
from src.silver.transform import transform_dimension, transform_hired_employees


def run_silver(spark: SparkSession) -> None:
    departments_bronze = spark.table("bronze.departments_raw")
    jobs_bronze = spark.table("bronze.jobs_raw")
    hired_bronze = spark.table("bronze.hired_employees_raw")

    departments_clean, departments_rejected = transform_dimension(departments_bronze, "departments", "department")
    jobs_clean, jobs_rejected = transform_dimension(jobs_bronze, "jobs", "job")

    merge_into_table(spark, departments_clean, "silver", "departments", merge_keys=["id"])
    merge_into_table(spark, jobs_clean, "silver", "jobs", merge_keys=["id"])

    # Referential checks are evaluated against the just-loaded Silver
    # dimensions (not Bronze): a department/job reference is only "valid" if
    # it itself passed the Silver rules.
    silver_departments = spark.table("silver.departments")
    silver_jobs = spark.table("silver.jobs")

    hired_clean, hired_rejected = transform_hired_employees(hired_bronze, silver_departments, silver_jobs)

    merge_into_table(
        spark,
        hired_clean,
        "silver",
        "hired_employees",
        merge_keys=["id"],
        table_properties={"delta.enableChangeDataFeed": "true"},
    )

    all_rejected = departments_rejected.unionByName(jobs_rejected).unionByName(hired_rejected)
    merge_into_table(
        spark,
        all_rejected,
        "silver",
        "rejected_records",
        merge_keys=["table_name", "record_id", "source_file"],
    )


if __name__ == "__main__":
    from src.common.spark_session import get_spark

    spark = get_spark("silver_transform")
    run_silver(spark)
    print("Silver transform complete.")
