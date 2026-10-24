"""
Silver customers (SQL Server CRM) - cleaning rules agreed in Class 13.

customer_name "GALLARDO, JOHN D" -> lower, split into last_name / first_name
city                              -> upper case
postcode "12345.0"                -> "12345"
valid_from / valid_to (epoch)     -> timestamp
country                           -> 'USA' (all customers are US)
"""
import pyspark.sql.functions as F

from common_utils.generic_functions import apply_standard_steps


def _epoch_to_timestamp(column):
    return F.expr(f"timestamp_seconds(try_cast(try_cast(`{column}` AS DOUBLE) AS BIGINT))")


def transform_customers(df, config):
    if "customer_name" in df.columns:
        df = (
            df.withColumn("customer_name", F.lower(F.trim(F.col("customer_name"))))
            .withColumn("name_parts", F.split(F.col("customer_name"), ","))
            # get() returns NULL when the index is missing (safe in ANSI mode)
            .withColumn("last_name", F.trim(F.expr("get(name_parts, 0)")))
            .withColumn("first_name", F.trim(F.expr("get(name_parts, 1)")))
        )

    if "city" in df.columns:
        df = df.withColumn("city", F.upper(F.trim(F.col("city"))))

    if "state" in df.columns:
        df = df.withColumn("state", F.upper(F.trim(F.col("state"))))

    if "postcode" in df.columns:
        df = df.withColumn(
            "postcode",
            F.regexp_replace(F.col("postcode").cast("string"), r"\.0$", ""),
        )

    for column in ("valid_from", "valid_to"):
        if column in df.columns:
            df = df.withColumn(column, _epoch_to_timestamp(column))

    df = df.withColumn("country", F.lit(config.get("default_country", "USA")))

    return apply_standard_steps(df, config)
