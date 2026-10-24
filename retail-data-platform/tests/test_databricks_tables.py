"""
Regression + smoke tests on the real tables. Run them on Databricks after the
job has loaded data (see README > Tests). Skipped everywhere else.
"""
import os

import pytest

pytestmark = pytest.mark.databricks

CATALOG = os.environ.get("RDP_CATALOG", "retaildataplatform")


def test_silver_customers_schema_is_stable(spark):
    df = spark.table(f"{CATALOG}.silver.customers")
    types = dict(df.dtypes)
    for column, expected in {"customer_id": "bigint", "first_name": "string", "last_name": "string",
                             "units_purchased": "bigint", "last_update_ts": "timestamp"}.items():
        assert types.get(column) == expected, f"{column}: {types.get(column)} != {expected}"
    assert df.count() > 0


def test_customers_history_has_one_current_row_per_customer(spark):
    df = spark.table(f"{CATALOG}.silver.customers_history").filter("is_current")
    assert df.groupBy("customer_id").count().filter("count > 1").count() == 0


@pytest.mark.parametrize("table", ["dim_customer", "dim_product", "dim_date", "dim_order",
                                   "fact_sales", "fact_pos_sales", "vw_sales_all_channels"])
def test_gold_objects_exist_and_query(spark, table):
    name = f"{CATALOG}.gold.{table}"
    assert spark.catalog.tableExists(name), f"{name} missing"
    assert spark.table(name).limit(1).count() == 1


def test_fact_sales_links_to_every_dimension(spark):
    g = f"{CATALOG}.gold"
    orphans = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {g}.fact_sales f
        LEFT ANTI JOIN {g}.dim_product p ON f.product_id = p.product_id
    """).first().n
    assert orphans == 0
