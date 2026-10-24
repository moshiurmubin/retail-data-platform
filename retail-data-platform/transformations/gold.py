"""
Gold layer - Kimball star schema (Class 3, 4, 5, 16).

Business process : sales
Grain            : fact_sales      = one row per online order line (order_number + product_id)
                   fact_pos_sales  = one row per store sale line (customer + product + date)
Dimensions       : dim_customer, dim_product, dim_date, dim_order
Measures         : qty, unit_price, discount, promo_quantity, total_amount

Kimball unknown members: a fact row whose customer / product / date cannot be
found in its dimension points to the "Unknown" row (-1 / 'UNKNOWN') instead
of being lost or breaking the foreign key.
"""
import pyspark.sql.functions as F

from common_utils.generic_functions import deduplicate, safe_cast, select_columns

UNKNOWN_CUSTOMER_ID = -1
UNKNOWN_PRODUCT_ID = "UNKNOWN"
UNKNOWN_DATE_KEY = -1


def date_key(column):
    """yyyyMMdd integer key, e.g. 2026-08-21 -> 20260821."""
    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


def finish(df, config):
    df = safe_cast(df, config.get("cast_columns"))
    return select_columns(df, config.get("select_columns"))


# ------------------------------------------------------------------ dimensions

def build_dim_date(spark, config):
    """One row per calendar day between start_date and end_date (SEQUENCE + EXPLODE, Class 5)."""
    start, end = config["transform"]["start_date"], config["transform"]["end_date"]

    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date"
    )

    df = (
        df.withColumn("date_key", date_key("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    )

    unknown = spark.createDataFrame(
        [(UNKNOWN_DATE_KEY, "1900-01-01", 1, 1, "Unknown", 1900, 1, 1, "Unknown", 1, False)],
        "date_key int, date string, day int, month int, month_name string, year int, "
        "quarter int, week int, day_name string, day_of_week int, is_weekend boolean",
    ).withColumn("date", F.to_date("date"))

    df = df.unionByName(unknown)
    return finish(df, config)


def build_dim_customer(spark, customers, config):
    """Current customers from Silver (SCD1 table) + an Unknown member."""
    default_type = config["transform"]["default_customer_type"]

    df = (
        customers.select(
            "customer_id", "first_name", "last_name", "city", "state",
            "loyalty_segment", "valid_from", "last_update_ts",
        )
        .withColumn(
            "full_name",
            F.initcap(F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name")))),
        )
        .withColumn(
            "customer_type",
            F.when(F.col("loyalty_segment").isNull(), F.lit(default_type))
            .otherwise(F.concat(F.lit("Segment_"), F.col("loyalty_segment").cast("string"))),
        )
        .withColumn("created_date", F.to_date("valid_from"))
        .withColumn("last_updated", F.col("last_update_ts"))
    )

    df = deduplicate(df, ["customer_id"], [{"column": "last_updated", "direction": "desc"}])
    df = finish(df, config)

    unknown = spark.createDataFrame(
        [(UNKNOWN_CUSTOMER_ID, "Unknown", "Unknown", "Unknown")],
        "customer_id long, first_name string, last_name string, full_name string",
    )
    for column in df.columns:
        if column not in unknown.columns:
            unknown = unknown.withColumn(column, F.lit(None).cast(df.schema[column].dataType))
    unknown = unknown.withColumn("customer_type", F.lit(default_type))

    return df.unionByName(unknown.select(*df.columns))


def build_dim_product(spark, order_products, sales, config):
    """
    Products from online order lines + store sales; category comes from store
    sales (the only source that has it). 'Unknown' when not known.
    """
    online = order_products.select(
        "product_id", "product_name", "unit", F.lit(None).cast("string").alias("category"),
        F.lit(2).alias("__priority"),
    )
    store = sales.select(
        "product_id",
        F.coalesce("product_name_json", "product_name").alias("product_name"),
        "unit",
        F.col("product_category").alias("category"),
        F.lit(1).alias("__priority"),
    )

    both = online.unionByName(store).filter(F.col("product_id").isNotNull())

    category = (
        both.filter(F.col("category").isNotNull())
        .groupBy("product_id").agg(F.first("category", ignorenulls=True).alias("category_found"))
    )

    df = deduplicate(both, ["product_id"], [{"column": "__priority", "direction": "asc"}])
    df = (
        df.join(category, "product_id", "left")
        .withColumn("category", F.coalesce("category", "category_found", F.lit("Unknown")))
        .drop("category_found", "__priority")
    )
    df = finish(df, config)

    unknown = spark.createDataFrame(
        [(UNKNOWN_PRODUCT_ID, "Unknown", "Unknown", "Unknown")],
        "product_id string, product_name string, unit string, category string",
    )
    return df.unionByName(unknown.select(*df.columns))


def build_dim_order(sales_orders, config):
    df = sales_orders.withColumn(
        "order_date_key", F.coalesce(date_key("order_timestamp"), F.lit(UNKNOWN_DATE_KEY))
    )
    df = deduplicate(df, ["order_number"], [{"column": "order_timestamp", "direction": "desc"}])
    return finish(df, config)


# ------------------------------------------------------------------ facts

def resolve_key(fact, fact_column, dim, dim_column, unknown_value):
    """Replace keys that do not exist in the dimension with the Unknown member."""
    keys = dim.select(F.col(dim_column).alias("__dim_key")).distinct()
    return (
        fact.join(keys, F.col(fact_column) == F.col("__dim_key"), "left")
        .withColumn(
            fact_column,
            F.when(F.col("__dim_key").isNull(), F.lit(unknown_value)).otherwise(F.col(fact_column)),
        )
        .drop("__dim_key")
    )


def prepare_promotions(promotions):
    """One promotion per order + product, otherwise the join would duplicate fact rows."""
    return (
        promotions.groupBy("order_number", "promo_product_id")
        .agg(
            F.max("discount").alias("discount"),
            F.max("promo_quantity").alias("promo_quantity"),
        )
    )


def build_fact_sales(sales_orders, order_products, promotions,
                     dim_customer, dim_product, dim_date, config):
    t = config["transform"]

    orders = sales_orders.select(
        "order_number", "customer_id", date_key("order_timestamp").alias("order_date_key")
    )

    lines = order_products.select(
        "order_number", "product_id", "qty", F.col("price").alias("unit_price")
    )

    promos = prepare_promotions(promotions)

    fact = (
        lines.alias("l")
        .join(orders.alias("o"), F.col("l.order_number") == F.col("o.order_number"), "left")
        .join(
            promos.alias("pr"),
            (F.col("l.order_number") == F.col("pr.order_number"))
            & (F.col("l.product_id") == F.col("pr.promo_product_id")),
            "left",
        )
        .select(
            F.col("l.order_number").alias("order_number"),
            F.col("o.customer_id").alias("customer_id"),
            F.col("l.product_id").alias("product_id"),
            F.col("o.order_date_key").alias("order_date_key"),
            F.col("l.qty").alias("qty"),
            F.col("l.unit_price").alias("unit_price"),
            F.coalesce("pr.discount", F.lit(t["default_discount"])).alias("discount"),
            F.coalesce("pr.promo_quantity", F.lit(t["default_promo_quantity"])).alias("promo_quantity"),
        )
        .withColumn(
            "total_amount",
            F.round(F.col("qty") * F.col("unit_price") * (F.lit(1) - F.col("discount")), 2),
        )
        .withColumn("channel", F.lit("ONLINE"))
    )

    fact = resolve_key(fact, "customer_id", dim_customer, "customer_id", UNKNOWN_CUSTOMER_ID)
    fact = resolve_key(fact, "product_id", dim_product, "product_id", UNKNOWN_PRODUCT_ID)
    fact = resolve_key(fact, "order_date_key", dim_date, "date_key", UNKNOWN_DATE_KEY)

    fact = deduplicate(fact, ["order_number", "product_id"], [{"column": "total_amount", "direction": "desc"}])
    return finish(fact, config)


def build_fact_pos_sales(sales, dim_customer, dim_product, dim_date, config):
    fact = sales.select(
        "customer_id",
        "product_id",
        date_key("order_date").alias("order_date_key"),
        F.col("quantity").alias("qty"),
        F.col("product_price").alias("unit_price"),
        F.round(F.col("total_price"), 2).alias("total_amount"),
        F.lit("STORE").alias("channel"),
    )

    fact = resolve_key(fact, "customer_id", dim_customer, "customer_id", UNKNOWN_CUSTOMER_ID)
    fact = resolve_key(fact, "product_id", dim_product, "product_id", UNKNOWN_PRODUCT_ID)
    fact = resolve_key(fact, "order_date_key", dim_date, "date_key", UNKNOWN_DATE_KEY)

    return finish(fact, config)
