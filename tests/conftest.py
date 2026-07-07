import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture(scope="session")
def spark():
    tmp_dir = tempfile.mkdtemp(prefix="etl_test_lakehouse_")
    os.environ["ETL_LAKEHOUSE_ROOT"] = tmp_dir

    from src.common.spark_session import get_spark

    spark_session = get_spark("pytest")
    yield spark_session
    spark_session.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)


def make_bronze_hired_employees(spark, rows):
    """rows: list of dicts with keys id, name, datetime, department_id,
    job_id, source_file; source_system/ingestion_timestamp/ingestion_date/
    batch_id get sensible defaults so tests only spell out what they care
    about.
    """
    now = datetime(2021, 1, 1, 0, 0, 0)
    full_rows = []
    for row in rows:
        full_rows.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "datetime": row.get("datetime"),
                "department_id": row.get("department_id"),
                "job_id": row.get("job_id"),
                "source_system": row.get("source_system", "landing_csv"),
                "source_file": row.get("source_file", "hired_employees_2021_01_01.csv"),
                "ingestion_timestamp": row.get("ingestion_timestamp", now),
                "ingestion_date": row.get("ingestion_date", now.date()),
                "batch_id": row.get("batch_id", "test-batch"),
            }
        )
    return spark.createDataFrame(full_rows)


def make_bronze_dimension(spark, rows, name_col):
    now = datetime(2021, 1, 1, 0, 0, 0)
    full_rows = []
    for row in rows:
        full_rows.append(
            {
                "id": row.get("id"),
                name_col: row.get(name_col),
                "source_system": row.get("source_system", "landing_csv"),
                "source_file": row.get("source_file", "departments_2021_01_01.csv"),
                "ingestion_timestamp": row.get("ingestion_timestamp", now),
                "ingestion_date": row.get("ingestion_date", now.date()),
                "batch_id": row.get("batch_id", "test-batch"),
            }
        )
    return spark.createDataFrame(full_rows)
