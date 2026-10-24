"""Silver transformations against Bronze-shaped samples (runs locally and in CI)."""
import pytest

from common_utils.data_quality import split_valid_invalid
from common_utils.generic_functions import deduplicate_from_config
from tests.sample_data import bronze_customers, bronze_s3_sales, bronze_sales_orders, cfg
from transformations.silver_customers import transform_customers
from transformations.silver_sales import transform_sales
from transformations.silver_sales_orders import TRANSFORMS, parse_sales_orders


@pytest.fixture(scope="module")
def customers(spark):
    config = cfg("silver/config/config_b2s_customers.json")
    cleaned = transform_customers(bronze_customers(spark), config)
    valid, invalid = split_valid_invalid(cleaned, config["quarantine"]["rules"])
    return config, deduplicate_from_config(valid, config), invalid


def test_customers_schema_follows_config(customers):
    config, df, _ = customers
    assert df.columns == config["select_columns"]
    types = dict(df.dtypes)
    assert types["customer_id"] == "bigint"
    assert types["valid_from"] == "timestamp"
    assert types["units_purchased"] == "bigint"


def test_customers_cleaning_rules(customers):
    _, df, _ = customers
    john = df.filter("customer_id = 16738781").first()
    assert (john.first_name, john.last_name) == ("john d", "gallardo")
    assert john.city == "MOUNTAIN VIEW"
    assert john.postcode == "94043"
    assert john.country == "USA"
    assert str(john.valid_from).startswith("2018-07-")

    madonna = df.filter("customer_id = 20000003").first()
    assert madonna.last_name == "madonna" and madonna.first_name is None


def test_customers_dedup_and_quarantine(customers):
    _, df, invalid = customers
    assert df.count() == 3
    assert df.groupBy("customer_id").count().filter("count > 1").count() == 0
    assert invalid.count() == 1
    assert invalid.first().dq_failed_rules == "customer_id_not_null"


@pytest.fixture(scope="module")
def sales_orders_tables(spark):
    config = cfg("silver/config/config_b2s_sales_orders.json")
    parsed = parse_sales_orders(bronze_sales_orders(spark))
    out = {}
    for name, table_config in config["tables"].items():
        transformed = TRANSFORMS[name](parsed, table_config)
        valid, invalid = split_valid_invalid(transformed, table_config["quarantine"]["rules"])
        out[name] = (deduplicate_from_config(valid, table_config), invalid, table_config)
    return out


def test_sales_orders_one_row_per_order(sales_orders_tables):
    df, invalid, config = sales_orders_tables["sales_orders"]
    assert df.columns == config["select_columns"]
    rows = {r.order_number: r for r in df.collect()}
    assert set(rows) == {317568043, 317568044}
    assert str(rows[317568043].order_timestamp).startswith("2022-06-08")
    assert str(rows[317568044].order_timestamp).startswith("2019-08-01")   # 1.564627663E9 parsed
    assert rows[317568044].customer_id == 99999999                         # "99999999" cast
    assert invalid.count() == 1                                            # order_number NULL


def test_order_products_flattened(sales_orders_tables):
    df, _, _ = sales_orders_tables["order_products"]
    lines = {(r.order_number, r.product_id): (r.qty, r.price) for r in df.collect()}
    assert lines == {
        (317568043, "P101"): (2, 1200.0),
        (317568043, "P102"): (1, 30.0),     # "30" and "1" arrived as strings
        (317568044, "P102"): (3, 30.0),
    }


def test_promotions_and_clicks(sales_orders_tables):
    promos, _, _ = sales_orders_tables["promotions"]
    assert [(r.promo_product_id, r.discount) for r in promos.collect()] == [("P101", 0.1)]

    clicks, _, _ = sales_orders_tables["clicked_items"]
    got = {(r.order_number, r.product_id): r.score for r in clicks.collect()}
    assert got == {(317568043, "P101"): 70, (317568043, "P103"): 98, (317568044, "P104"): None}


def test_store_sales_flattened(spark):
    config = cfg("silver/config/config_b2s_sales.json")
    transformed = transform_sales(bronze_s3_sales(spark), config)
    valid, invalid = split_valid_invalid(transformed, config["quarantine"]["rules"])
    df = deduplicate_from_config(valid, config)

    assert df.columns == config["select_columns"]
    assert df.count() == 2 and invalid.count() == 1
    cable = df.filter("product_id = 'P200'").first()
    assert (cable.product_price, cable.quantity, str(cable.order_date)) == (5.5, 2, "2022-06-09")
