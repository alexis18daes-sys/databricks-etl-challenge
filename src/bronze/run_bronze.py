"""Orchestrates the Bronze extraction step. Can be run standalone or imported."""

import uuid

from pyspark.sql import SparkSession

from src.bronze.ingest import ingest_departments, ingest_hired_employees, ingest_jobs


def run_bronze(spark: SparkSession, batch_id: str = None) -> str:
    batch_id = batch_id or str(uuid.uuid4())
    ingest_departments(spark, batch_id)
    ingest_jobs(spark, batch_id)
    ingest_hired_employees(spark, batch_id)
    return batch_id


if __name__ == "__main__":
    from src.common.spark_session import get_spark

    spark = get_spark("bronze_extract")
    run_id = run_bronze(spark)
    print(f"Bronze ingestion complete. batch_id={run_id}")
