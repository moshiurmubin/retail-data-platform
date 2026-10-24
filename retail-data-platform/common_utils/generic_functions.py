import pyspark.sql.functions as F
from pyspark.sql.window import Window

from common_utils.logging import get_logger

logger = get_logger("generic_functions")

INTEGRAL_TYPES = {"byte", "short", "int", "integer", "long", "bigint"}


def rename_columns(df, mapping):
    """Rename columns from a dict {old: new}. Missing columns are ignored."""
    for old_name, new_name in (mapping or {}).items():
        if old_name in df.columns:
            logger.info("Renaming column [%s] to [%s]", old_name, new_name)
            df = df.withColumnRenamed(old_name, new_name)
    return df


def safe_cast(df, mapping):
    """
    Cast columns with try_cast so bad values become NULL instead of failing
    (Databricks serverless runs in ANSI mode, where a plain cast raises).

    Integral types are cast through DOUBLE first so '34.0' becomes 34.
    """
    for column, target_type in (mapping or {}).items():
        if column not in df.columns:
            logger.warning("Column [%s] not found. Skipping cast.", column)
            continue

        if target_type.lower() in INTEGRAL_TYPES:
            expression = f"try_cast(try_cast(`{column}` AS DOUBLE) AS {target_type})"
        else:
            expression = f"try_cast(`{column}` AS {target_type})"

        df = df.withColumn(column, F.expr(expression))
    return df


def drop_columns(df, columns):
    """Drop the listed columns that exist."""
    existing = [c for c in (columns or []) if c in df.columns]
    if existing:
        logger.info("Dropping columns %s", existing)
        df = df.drop(*existing)
    return df


def select_columns(df, columns):
    """Select the final column list (keeps the order defined in config)."""
    return df.select(*columns) if columns else df


def deduplicate(df, key_columns, order_by=None):
    """
    Keep one row per key. order_by = list of
    {"column": "...", "direction": "asc|desc", "nulls": "first|last"};
    the first row after ordering is kept.
    """
    if not key_columns:
        return df.dropDuplicates()

    order_expressions = []
    for item in order_by or []:
        col = F.col(item["column"])
        desc = item.get("direction", "asc").lower() == "desc"
        nulls_last = item.get("nulls", "last").lower() == "last"
        if desc:
            col = col.desc_nulls_last() if nulls_last else col.desc_nulls_first()
        else:
            col = col.asc_nulls_last() if nulls_last else col.asc_nulls_first()
        order_expressions.append(col)

    if not order_expressions:
        order_expressions = [F.lit(1)]

    window = Window.partitionBy(*key_columns).orderBy(*order_expressions)

    return (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def apply_standard_steps(df, config):
    """rename -> drop -> cast -> select, all driven by config.
    (Deduplication is a separate step so bad rows can be quarantined first.)"""
    df = rename_columns(df, config.get("rename_columns"))
    df = drop_columns(df, config.get("drop_columns"))
    df = safe_cast(df, config.get("cast_columns"))
    return select_columns(df, config.get("select_columns"))


def deduplicate_from_config(df, config):
    """Run deduplicate() with the "deduplication" block of a config, if enabled."""
    dedup = config.get("deduplication", {})
    if not dedup.get("enabled", False):
        return df
    logger.info("Deduplicating on %s", dedup["key_columns"])
    return deduplicate(df, dedup["key_columns"], dedup.get("order_by"))


def write_delta(spark, df, target_table, mode="overwrite", overwrite_schema=True):
    """Write a dataframe to a Delta table and return the row count of the table."""
    writer = df.write.format("delta").mode(mode)
    if overwrite_schema and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(target_table)

    rows = spark.read.table(target_table).count()
    logger.info("Wrote %s (%s rows)", target_table, rows)
    return rows


def assert_not_empty_and_unique(df, key_columns, name):
    """Fail fast before writing a table: it must have rows and a unique key."""
    rows = df.count()
    if rows == 0:
        raise ValueError(f"{name} has zero rows")

    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    if duplicates > 0:
        raise ValueError(f"{name} has {duplicates} duplicate keys on {key_columns}")

    logger.info("%s: %s rows, key %s unique", name, rows, key_columns)
    return rows
