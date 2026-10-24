"""
pytest setup.

- On Databricks (DATABRICKS_RUNTIME_VERSION is set) the existing Spark session is reused.
- Locally / in CI a small Spark session with Delta Lake is started, in ANSI mode
  like Databricks serverless, so unit tests catch the same casting errors.
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


def pytest_configure(config):
    config.addinivalue_line("markers", "databricks: needs the real Databricks tables")


def pytest_collection_modifyitems(config, items):
    if ON_DATABRICKS:
        return
    skip = pytest.mark.skip(reason="runs only on Databricks")
    for item in items:
        if "databricks" in item.keywords:
            item.add_marker(skip)


def _local_builder(warehouse, with_delta):
    from pyspark.sql import SparkSession

    # Builder options live on the class; clear them so a failed Delta attempt
    # does not leak spark.jars.packages into the plain fallback session.
    getattr(SparkSession.Builder, "_options", {}).clear()

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retail-data-platform-tests")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        builder = configure_spark_with_delta_pip(builder)
    return builder


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if ON_DATABRICKS:
        session = SparkSession.builder.getOrCreate()
        session.has_delta = True
        yield session
        return

    warehouse = tempfile.mkdtemp(prefix="rdp-warehouse-")
    try:
        # Delta jars are downloaded from Maven the first time
        session = _local_builder(warehouse, with_delta=True).getOrCreate()
        session.has_delta = True
    except Exception:
        # No Maven access: plain Spark, Delta (MERGE / SCD) tests are skipped
        session = _local_builder(warehouse, with_delta=False).getOrCreate()
        session.has_delta = False

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def delta_spark(spark):
    if not getattr(spark, "has_delta", False):
        pytest.skip("Delta Lake not available in this local Spark session")
    return spark
