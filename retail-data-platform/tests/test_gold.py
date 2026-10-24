"""Gold star-schema builders, fed by the Silver outputs of the samples."""
import pytest

from common_utils.data_quality import split_valid_invalid
from common_utils.generic_functions import deduplicate_from_config
from tests.sample_data import bronze_customers, bronze_s3_sales, bronze_sales_orders, cfg
from transformations import gold
from transformations.silver_customers import transform_customers
from transformations.silver_sales import transform_sales
from transformations.silver_sales_orders import TRANSFORMS, parse_sales_orders


def _clean(df, config):
    valid, _ = split_valid_invalid(df, config["quarantine"]["rules"])
    return deduplicate_from_config(valid, config)


@pytest.fixture(scope="module")
def silver(spark):
    c = cfg("silver/config/config_b2s_customers.json")
    so = cfg("silver/config/config_b2s_sales_orders.json")
    s = cfg("silver/config/config_b2s_sales.json")
    parsed = parse_sales_orders(bronze_sales_orders(spark))
    out = {"customers": _clean(transform_customers(bronze_customers(spark), c), c),
           "sales": _clean(transform_sales(bronze_s3_sales(spark), s), s)}
    for name, t in so["tables"].items():
        out[name] = _clean(TRANSFORMS[name](parsed, t), t)
    return out


@pytest.fixture(scope="module")
def dims(spark, silver):
    return {
        "dim_date": gold.build_dim_date(spark, cfg("gold/config/config_dim_date.json")),
        "dim_customer": gold.build_dim_customer(spark, silver["customers"], cfg("gold/config/config_dim_customer.json")),
        "dim_product": gold.build_dim_product(spark, silver["order_products"], silver["sales"],
                                              cfg("gold/config/config_dim_product.json")),
        "dim_order": gold.build_dim_order(silver["sales_orders"], cfg("gold/config/config_dim_order.json")),
    }


def _unique(df, keys):
    return df.groupBy(*keys).count().filter("count > 1").count() == 0


def test_dim_date(dims):
    d = dims["dim_date"]
    assert _unique(d, ["date_key"])
    row = d.filter("date_key = 20260821").first()
    assert (row.year, row.quarter, row.month_name, row.day_name, row.is_weekend) == (2026, 3, "August", "Friday", False)
    assert d.filter("date_key = -1").count() == 1


def test_dim_customer(dims):
    d = dims["dim_customer"]
    assert d.columns == cfg("gold/config/config_dim_customer.json")["select_columns"]
    assert _unique(d, ["customer_id"])
    john = d.filter("customer_id = 16738781").first()
    assert (john.full_name, john.customer_type, str(john.created_date)) == ("John D Gallardo", "Segment_1", "2018-07-21")
    assert d.filter("customer_id = 20000003").first().customer_type == "Unknown"
    assert d.filter("customer_id = -1").first().full_name == "Unknown"


def test_dim_product_conformed(dims):
    d = dims["dim_product"]
    assert _unique(d, ["product_id"])
    cats = {r.product_id: r.category for r in d.collect()}
    assert cats["P101"] == "Computers"      # online product, category from store sales
    assert cats["P102"] == "Unknown"        # never sold in store
    assert cats["P200"] == "Accessories"    # store-only product
    assert "UNKNOWN" in cats


def test_dim_order(dims):
    d = dims["dim_order"]
    assert _unique(d, ["order_number"])
    assert {r.order_number: r.order_date_key for r in d.collect()} == {317568043: 20220608, 317568044: 20190801}


def test_fact_sales_measures_and_unknown_members(silver, dims):
    fact = gold.build_fact_sales(
        silver["sales_orders"], silver["order_products"], silver["promotions"],
        dims["dim_customer"], dims["dim_product"], dims["dim_date"],
        cfg("gold/config/config_fact_sales.json"),
    )
    assert _unique(fact, ["order_number", "product_id"])
    rows = {(r.order_number, r.product_id): r for r in fact.collect()}
    assert len(rows) == 3

    laptop = rows[(317568043, "P101")]
    assert (laptop.discount, laptop.total_amount, laptop.order_date_key) == (0.1, 2160.0, 20220608)

    mouse = rows[(317568043, "P102")]
    assert (mouse.discount, mouse.promo_quantity, mouse.total_amount) == (0.0, 0, 30.0)

    ghost = rows[(317568044, "P102")]
    assert ghost.customer_id == gold.UNKNOWN_CUSTOMER_ID      # customer not in CRM

    # every key exists in its dimension
    for col, dim, key in [("customer_id", "dim_customer", "customer_id"),
                          ("product_id", "dim_product", "product_id"),
                          ("order_date_key", "dim_date", "date_key")]:
        assert fact.join(dims[dim], fact[col] == dims[dim][key], "left_anti").count() == 0


def test_fact_pos_sales(silver, dims):
    fact = gold.build_fact_pos_sales(
        silver["sales"], dims["dim_customer"], dims["dim_product"], dims["dim_date"],
        cfg("gold/config/config_fact_pos_sales.json"),
    )
    assert fact.count() == 2
    assert set(r.channel for r in fact.collect()) == {"STORE"}
    assert fact.filter("order_date_key = 20220609").first().total_amount == 11.0


def test_data_quality_configs_pass_on_clean_sample(silver, dims):
    """Every Gold DQ config runs against the sample star schema with no FAIL."""
    from common_utils.data_quality import run_data_quality

    fact_sales = gold.build_fact_sales(
        silver["sales_orders"], silver["order_products"], silver["promotions"],
        dims["dim_customer"], dims["dim_product"], dims["dim_date"],
        cfg("gold/config/config_fact_sales.json"),
    )
    fact_pos = gold.build_fact_pos_sales(
        silver["sales"], dims["dim_customer"], dims["dim_product"], dims["dim_date"],
        cfg("gold/config/config_fact_pos_sales.json"),
    )
    cache = {f"test.gold.{k}": v for k, v in dims.items()}
    cache["test.gold.fact_sales"] = fact_sales
    cache["test.gold.fact_pos_sales"] = fact_pos
    cache.update({f"test.silver.{k}": v for k, v in silver.items()})

    failures = []
    for name in ["dim_customer", "dim_product", "dim_date", "dim_order", "fact_sales", "fact_pos_sales",
                 "customers", "sales_orders", "order_products", "promotions", "clicked_items", "sales"]:
        config = cfg(f"dq/config/config_dq_{name}.json")
        for result in run_data_quality(cache[config["table"]], config, cache):
            if result["status"] == "FAIL":
                failures.append((name, result["check_name"], result["message"]))
    assert failures == []
