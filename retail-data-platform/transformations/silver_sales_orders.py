"""
Silver sales orders (Cosmos DB web shop) - semi-structured JSON, Class 14.

One Bronze row = one JSON document in column json_data:
    order_number, customer_id, customer_name, number_of_line_items,
    order_datetime (epoch), ordered_products[], promo_info[], clicked_items[][]

from_json() -> explode() -> select nested fields gives four flat Silver tables:
    sales_orders     one row per order
    order_products   one row per order line
    promotions       one row per promotion on an order
    clicked_items    one row per clicked product
"""
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from common_utils.generic_functions import apply_standard_steps

# Every leaf is read as STRING: Spark keeps numbers as their text, and the
# config casts them afterwards with try_cast. This survives a source that
# sends "12" one day and 12 the next.
PRODUCT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("price", StringType()),
    StructField("curr", StringType()),
    StructField("qty", StringType()),
    StructField("unit", StringType()),
    StructField("promotion_info", StringType()),
])

PROMOTION_SCHEMA = StructType([
    StructField("promo_id", StringType()),
    StructField("promo_item", StringType()),
    StructField("promo_disc", StringType()),
    StructField("promo_qty", StringType()),
])

SALES_ORDER_SCHEMA = StructType([
    StructField("order_number", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("number_of_line_items", StringType()),
    StructField("order_datetime", StringType()),
    StructField("ordered_products", ArrayType(PRODUCT_SCHEMA)),
    StructField("promo_info", ArrayType(PROMOTION_SCHEMA)),
    StructField("clicked_items", ArrayType(ArrayType(StringType()))),
])


def parse_sales_orders(df, json_column="json_data"):
    """JSON string -> struct -> top-level columns (+ keeps last_update_ts)."""
    keep = [c for c in ("last_update_ts",) if c in df.columns]
    return (
        df.withColumn("data", F.from_json(F.col(json_column), SALES_ORDER_SCHEMA))
        .select("data.*", *keep)
    )


def transform_sales_orders(parsed, config):
    df = parsed.withColumn(
        "order_timestamp",
        F.expr("timestamp_seconds(try_cast(try_cast(order_datetime AS DOUBLE) AS BIGINT))"),
    ).withColumn("customer_name", F.lower(F.trim(F.col("customer_name"))))
    return apply_standard_steps(df, config)


def transform_order_products(parsed, config):
    df = (
        parsed.withColumn("p", F.explode_outer("ordered_products"))
        .select(
            "order_number",
            "customer_id",
            F.col("p.id").alias("product_id"),
            F.col("p.name").alias("product_name"),
            F.col("p.price").alias("price"),
            F.col("p.curr").alias("currency"),
            F.col("p.qty").alias("qty"),
            F.col("p.unit").alias("unit"),
            *[c for c in ("last_update_ts",) if c in parsed.columns],
        )
        .filter(F.col("product_id").isNotNull())
    )
    return apply_standard_steps(df, config)


def transform_promotions(parsed, config):
    df = (
        parsed.withColumn("pr", F.explode_outer("promo_info"))
        .select(
            "order_number",
            "customer_id",
            F.col("pr.promo_id").alias("promo_id"),
            F.col("pr.promo_item").alias("promo_product_id"),
            F.col("pr.promo_disc").alias("discount"),
            F.col("pr.promo_qty").alias("promo_quantity"),
            *[c for c in ("last_update_ts",) if c in parsed.columns],
        )
        .filter(F.col("promo_id").isNotNull())
    )
    return apply_standard_steps(df, config)


def transform_clicked_items(parsed, config):
    df = (
        parsed.withColumn("ci", F.explode_outer("clicked_items"))
        .select(
            "order_number",
            "customer_id",
            # get() is NULL-safe; ci[0] would fail in ANSI mode on a short array
            F.expr("get(ci, 0)").alias("product_id"),
            F.expr("get(ci, 1)").alias("score"),
            *[c for c in ("last_update_ts",) if c in parsed.columns],
        )
        .filter(F.col("product_id").isNotNull())
    )
    return apply_standard_steps(df, config)


TRANSFORMS = {
    "sales_orders": transform_sales_orders,
    "order_products": transform_order_products,
    "promotions": transform_promotions,
    "clicked_items": transform_clicked_items,
}
