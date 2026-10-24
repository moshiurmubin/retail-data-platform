"""
SCD Type 1 and Type 2 on real Delta tables.
Runs in CI (GitHub Actions downloads Delta) and on Databricks; skipped when
Delta is not available locally.
"""
import os
import uuid

import pytest

from common_utils.scdfunctions import scd_type_1, scd_type_2

SCHEMA = "customer_id long, city string, segment string"


def _table(spark):
    on_dbx = "DATABRICKS_RUNTIME_VERSION" in os.environ
    catalog = os.environ.get("RDP_CATALOG", "retaildataplatform")
    prefix = f"{catalog}.silver." if on_dbx else "default."
    return f"{prefix}scd_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def table(delta_spark):
    name = _table(delta_spark)
    yield name
    delta_spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_scd1_creates_then_updates_and_inserts(delta_spark, table):
    spark = delta_spark
    assert scd_type_1(spark, spark.createDataFrame([(1, "COVENTRY", "1")], SCHEMA), table, ["customer_id"]) == "created"

    changes = spark.createDataFrame([(1, "LONDON", "1"), (2, "LEEDS", "2")], SCHEMA)
    assert scd_type_1(spark, changes, table, ["customer_id"]) == "merged"

    rows = {r.customer_id: r.city for r in spark.read.table(table).collect()}
    assert rows == {1: "LONDON", 2: "LEEDS"}


def test_scd2_keeps_history(delta_spark, table):
    spark = delta_spark
    tracked = ["city", "segment"]

    scd_type_2(spark, spark.createDataFrame([(1, "COVENTRY", "1"), (2, "LEEDS", "2")], SCHEMA),
               table, ["customer_id"], tracked, effective_ts="2026-04-01 00:00:00")

    # customer 1 moves, customer 2 unchanged, customer 3 is new
    scd_type_2(spark, spark.createDataFrame([(1, "LONDON", "1"), (2, "LEEDS", "2"), (3, "YORK", "3")], SCHEMA),
               table, ["customer_id"], tracked, effective_ts="2026-09-01 00:00:00")

    # re-running the same input changes nothing (idempotent)
    scd_type_2(spark, spark.createDataFrame([(1, "LONDON", "1"), (2, "LEEDS", "2"), (3, "YORK", "3")], SCHEMA),
               table, ["customer_id"], tracked, effective_ts="2026-09-02 00:00:00")

    rows = spark.read.table(table).orderBy("customer_id", "effective_from").collect()
    got = [(r.customer_id, r.city, r.is_current, str(r.effective_to)[:10] if r.effective_to else None) for r in rows]
    assert got == [
        (1, "COVENTRY", False, "2026-09-01"),
        (1, "LONDON", True, None),
        (2, "LEEDS", True, None),
        (3, "YORK", True, None),
    ]
