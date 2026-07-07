"""Builds the Gold layer tables from the SparkSQL definitions in sql/."""

import os

from pyspark.sql import SparkSession

from src.common.sql_runner import run_sql_file

_SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "sql")

_GOLD_SQL_FILES = [
    "gold_metric_1_hires_by_department_job_quarter.sql",
    "gold_metric_2_departments_above_average.sql",
]


def run_gold(spark: SparkSession) -> None:
    for file_name in _GOLD_SQL_FILES:
        run_sql_file(spark, os.path.join(_SQL_DIR, file_name))


if __name__ == "__main__":
    from src.common.spark_session import get_spark

    spark = get_spark("gold_build")
    run_gold(spark)
    print("Gold build complete.")
