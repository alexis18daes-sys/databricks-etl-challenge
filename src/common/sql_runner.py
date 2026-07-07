"""Tiny helper to execute a plain .sql file (one or more statements) through Spark SQL."""

from pyspark.sql import DataFrame, SparkSession


def _strip_line_comments(sql_text: str) -> str:
    return "\n".join(line for line in sql_text.splitlines() if not line.strip().startswith("--"))


def run_sql_file(spark: SparkSession, path: str, verbose: bool = False) -> DataFrame:
    """Execute every `;`-separated statement in a .sql file, in order.

    Returns the result of the last statement (handy for validation queries
    where the file ends with the SELECT you care about).
    """
    with open(path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    statements = [s.strip() for s in _strip_line_comments(sql_text).split(";") if s.strip()]
    result = None
    for statement in statements:
        result = spark.sql(statement)
        if verbose:
            result.show(truncate=False)
    return result
