"""Explicit schemas for the raw source data (CSV batches and JSON API pages)."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DEPARTMENTS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("department", StringType(), True),
    ]
)

JOBS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True),
    ]
)

# Shared by the CSV batch files and the exploded `data` array from the API payloads.
HIRED_EMPLOYEES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("job_id", IntegerType(), True),
    ]
)

# Top-level envelope of the paginated mock API responses.
HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA = StructType(
    [
        StructField("page", IntegerType(), True),
        StructField("has_next", BooleanType(), True),
        StructField("source_system", StringType(), True),
        StructField("data", ArrayType(HIRED_EMPLOYEES_SCHEMA), True),
    ]
)
