"""Prints the validation/reconciliation and Delta audit query results.

Usage (after scripts/run_pipeline.py has populated the tables):
    python scripts/run_validation.py
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.common.spark_session import get_spark  # noqa: E402
from src.common.sql_runner import run_sql_file  # noqa: E402


def main() -> None:
    spark = get_spark("hiring_etl_validation")
    try:
        print("\n================ VALIDATION & RECONCILIATION ================")
        run_sql_file(spark, str(_ROOT / "sql" / "validation_queries.sql"), verbose=True)
        print("\n================ DELTA AUDIT ================")
        run_sql_file(spark, str(_ROOT / "sql" / "delta_audit_queries.sql"), verbose=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
