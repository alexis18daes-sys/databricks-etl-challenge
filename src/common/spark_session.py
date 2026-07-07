"""Spark session factory shared by local execution and Databricks notebooks."""

import os
import sys

from pyspark.sql import SparkSession

_LOCAL_JARS_DIR = os.path.join(os.path.expanduser("~"), "tools", "jars")


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "hiring_etl") -> SparkSession:
    """Return a SparkSession configured for Delta Lake.

    On Databricks, Delta and the Hive/Unity Catalog metastore are already
    configured by the runtime, so we simply reuse the managed session.
    Locally, we point Spark at the Delta jars pulled from Maven Central
    (see README "Running locally" section) and a local warehouse directory,
    so bronze/silver/gold schemas and tables behave the same way they would
    on a Databricks cluster.
    """
    builder = SparkSession.builder.appName(app_name)

    if is_databricks():
        return builder.getOrCreate()

    # On Windows, the bare "python"/"python3" command can resolve to the
    # Microsoft Store alias stub instead of a real interpreter, which makes
    # every PySpark executor's Python worker fail to start (only shows up
    # once a job needs Python workers, e.g. shuffles/joins - simple reads
    # and writes stay entirely in the JVM and don't hit this). Pin both to
    # this exact interpreter to avoid the alias.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    delta_jars = ",".join(
        os.path.join(_LOCAL_JARS_DIR, jar) for jar in ("delta-spark_2.12-3.2.0.jar", "delta-storage-3.2.0.jar")
    )
    lakehouse_root = os.environ.get("ETL_LAKEHOUSE_ROOT", os.path.join(os.getcwd(), "_lakehouse"))
    warehouse_dir = os.path.join(lakehouse_root, "_spark-warehouse")
    metastore_dir = os.path.join(lakehouse_root, "_metastore_db")
    builder = (
        builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars", delta_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        # Schemas/tables (bronze/silver/gold) must survive across separate
        # `python -m ...` runs, not just within one SparkSession. That needs a
        # persistent (on-disk) Hive metastore instead of Spark's default
        # in-memory catalog, which is thrown away when the JVM exits.
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_dir};create=true")
        .enableHiveSupport()
        # Windows-only workaround: the native snappy JNI library gets blocked
        # when extracted to %TEMP% on this machine. Databricks (Linux) is not
        # affected and uses the default snappy codec.
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
