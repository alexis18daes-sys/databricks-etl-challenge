"""Local end-to-end entrypoint: Bronze -> Silver -> Gold.

Usage (from the project root, with JAVA_HOME/HADOOP_HOME set - see README):
    python scripts/run_pipeline.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.bronze.run_bronze import run_bronze  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402
from src.gold.build_gold import run_gold  # noqa: E402
from src.silver.run_silver import run_silver  # noqa: E402


def main() -> None:
    spark = get_spark("hiring_etl_pipeline")
    try:
        batch_id = run_bronze(spark)
        print(f"[bronze] done. batch_id={batch_id}")
        run_silver(spark)
        print("[silver] done.")
        run_gold(spark)
        print("[gold] done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
