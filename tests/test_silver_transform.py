from src.silver.transform import transform_dimension, transform_hired_employees
from tests.conftest import make_bronze_dimension, make_bronze_hired_employees


def test_dimension_rejects_null_id_and_empty_name(spark):
    bronze = make_bronze_dimension(
        spark,
        [
            {"id": 1, "department": "Engineering"},
            {"id": None, "department": "Orphan"},
            {"id": 2, "department": ""},
        ],
        name_col="department",
    )

    clean, rejected = transform_dimension(bronze, "departments", "department")

    assert [row["id"] for row in clean.collect()] == [1]
    reasons = {row["record_id"]: row["rejection_reason"] for row in rejected.collect()}
    assert reasons[None] == "id_is_null"
    assert reasons["2"] == "department_is_null_or_empty"


def test_dimension_dedupe_keeps_latest_by_source_file_date(spark):
    bronze = make_bronze_dimension(
        spark,
        [
            {"id": 1, "department": "Old Name", "source_file": "departments_2021_01_01.csv"},
            {"id": 1, "department": "New Name", "source_file": "departments_2021_01_02.csv"},
        ],
        name_col="department",
    )

    clean, rejected = transform_dimension(bronze, "departments", "department")

    result = clean.collect()
    assert len(result) == 1
    assert result[0]["department"] == "New Name"
    assert rejected.count() == 1
    assert rejected.collect()[0]["rejection_reason"] == "duplicate_id_superseded_by_newer_record"


def _departments(spark):
    return make_bronze_dimension(spark, [{"id": 1, "department": "Engineering"}], name_col="department").select(
        "id", "department"
    )


def _jobs(spark):
    return make_bronze_dimension(spark, [{"id": 1, "job": "Engineer"}], name_col="job").select("id", "job")


def test_hired_employees_rejects_invalid_records(spark):
    bronze = make_bronze_hired_employees(
        spark,
        [
            {"id": 1, "name": "Valid Person", "datetime": "2021-05-01T10:00:00Z", "department_id": 1, "job_id": 1},
            {"id": 2, "name": None, "datetime": "2021-05-01T10:00:00Z", "department_id": 1, "job_id": 1},
            {"id": 3, "name": "Bad Date", "datetime": "not_a_date", "department_id": 1, "job_id": 1},
            {"id": 4, "name": "Missing Dept", "datetime": "2021-05-01T10:00:00Z", "department_id": 99, "job_id": 1},
            {"id": 5, "name": "Null Job", "datetime": "2021-05-01T10:00:00Z", "department_id": 1, "job_id": None},
        ],
    )

    clean, rejected = transform_hired_employees(bronze, _departments(spark), _jobs(spark))

    assert [row["id"] for row in clean.collect()] == [1]
    reasons = {row["record_id"]: row["rejection_reason"] for row in rejected.collect()}
    assert reasons["2"] == "name_is_null_or_empty"
    assert reasons["3"] == "datetime_invalid_iso_format"
    assert reasons["4"] == "department_id_not_found_in_departments"
    assert reasons["5"] == "job_id_is_null"


def test_hired_employees_dedupe_prefers_later_source_file_date(spark):
    bronze = make_bronze_hired_employees(
        spark,
        [
            {
                "id": 42,
                "name": "Old Record",
                "datetime": "2021-05-01T10:00:00Z",
                "department_id": 1,
                "job_id": 1,
                "source_file": "hired_employees_2021_01_01.csv",
            },
            {
                "id": 42,
                "name": "Corrected Record",
                "datetime": "2021-05-01T10:00:00Z",
                "department_id": 1,
                "job_id": 1,
                "source_file": "hired_employees_2021_01_02.csv",
            },
        ],
    )

    clean, rejected = transform_hired_employees(bronze, _departments(spark), _jobs(spark))

    result = clean.collect()
    assert len(result) == 1
    assert result[0]["name"] == "Corrected Record"
    assert rejected.count() == 1
    assert rejected.collect()[0]["rejection_reason"] == "duplicate_id_superseded_by_newer_record"
