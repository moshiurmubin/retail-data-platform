"""
Silver sales (Amazon S3, physical-store POS) - Class 14.

The S3 parquet is flat except column `product`, a JSON string:
    {"curr":"USD","id":"...","name":"...","price":..,"qty":..,"unit":"..."}
from_json() turns it into a struct and its fields become columns.
"""
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common_utils.generic_functions import apply_standard_steps

PRODUCT_SCHEMA = StructType([
    StructField("curr", StringType()),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("price", StringType()),
    StructField("qty", StringType()),
    StructField("unit", StringType()),
])


def transform_sales(df, config):
    df = df.withColumn("product", F.from_json(F.col("product"), PRODUCT_SCHEMA))

    df = df.select(
        "customer_id",
        F.lower(F.trim(F.col("customer_name"))).alias("customer_name"),
        "product_name",
        "order_date",
        "product_category",
        F.col("product.curr").alias("currency"),
        F.col("product.id").alias("product_id"),
        F.col("product.name").alias("product_name_json"),
        F.col("product.price").alias("product_price"),
        F.col("product.qty").alias("quantity"),
        F.col("product.unit").alias("unit"),
        "total_price",
        "last_update_ts",
    )
    return apply_standard_steps(df, config)
