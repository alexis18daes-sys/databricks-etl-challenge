"""Centralized path configuration for the pipeline.

Locally the pipeline reads from the `data/` folder shipped with this repo and
writes Delta tables under `_lakehouse/`. On Databricks, override
``ETL_DATA_ROOT`` and ``ETL_LAKEHOUSE_ROOT`` (e.g. via job parameters or a
notebook widget) to point at a Unity Catalog Volume / DBFS mount, such as
``/Volumes/main/hiring/landing`` and ``/Volumes/main/hiring/lakehouse``. No
code changes are required to move between environments.
"""

import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_ROOT = os.environ.get("ETL_DATA_ROOT", os.path.join(_PROJECT_ROOT, "data"))
LAKEHOUSE_ROOT = os.environ.get("ETL_LAKEHOUSE_ROOT", os.path.join(_PROJECT_ROOT, "_lakehouse"))


def landing_path(*parts: str) -> str:
    return os.path.join(DATA_ROOT, "landing", *parts).replace("\\", "/")


def api_path(*parts: str) -> str:
    return os.path.join(DATA_ROOT, "api", *parts).replace("\\", "/")


def table_path(layer: str, table_name: str) -> str:
    """Storage location for a Delta table, e.g. table_path("bronze", "jobs_raw")."""
    return os.path.join(LAKEHOUSE_ROOT, layer, table_name).replace("\\", "/")
