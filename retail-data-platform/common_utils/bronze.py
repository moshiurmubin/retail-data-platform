import pyspark.sql.functions as F

from common_utils.logging import get_logger

logger = get_logger("bronze")


def read_raw(spark, raw_path, file_format, options=None):
    """
    Read raw files from the volume

    raw_path    : /Volumes/<catalog>/bronze/raw_data/<folder>/load_date=YYYY-MM-DD
    file_format : json | csv | parquet
    options     : reader options

    returns a dataframe
    """
    logger.info("Reading %s files from %s", file_format, raw_path)

    reader = spark.read.format(file_format)
    for key, value in (options or {}).items():
        reader = reader.option(key, value)

    return reader.load(raw_path)


def add_audit_columns(df, load_date, source_system):
    """
    Add the technical columns that make every Bronze row traceable

    last_update_ts : WHEN we processed it
    file_path      : WHICH raw file it came from
    load_date      : WHICH ingestion run it belongs to
    source_system  : WHICH source system sent it
    """
    return (
        df.withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("load_date", F.to_date(F.lit(load_date)))
        .withColumn("source_system", F.lit(source_system))
    )


def read_bronze_for_date(spark, table, load_date):
    """
    Read one ingestion run (load_date) from a Bronze table.
    If that date has no rows (e.g. a manual re-run on another day), fall back
    to the latest load_date so Silver always processes the newest data.
    """
    df = spark.read.table(table)
    run_df = df.filter(F.col("load_date") == F.to_date(F.lit(load_date)))

    if run_df.limit(1).count() > 0:
        logger.info("Reading %s for load_date=%s", table, load_date)
        return run_df

    latest = df.agg(F.max("load_date").alias("d")).first()["d"]
    if latest is None:
        raise ValueError(f"{table} is empty - run ingestion and Bronze first")

    logger.warning("No rows in %s for load_date=%s, using latest load_date=%s", table, load_date, latest)
    return df.filter(F.col("load_date") == F.lit(latest))


def bronze_ingestor(spark, df, target_table, load_date, source_system):
    """
    Write a raw dataframe into a Bronze Delta table.

    Idempotent: only the partition for this load_date is replaced, so
    re-running the same day never duplicates rows, and older days are kept.

    returns the number of rows written
    """
    df = add_audit_columns(df, load_date, source_system)

    writer = df.write.format("delta").option("mergeSchema", "true")

    if spark.catalog.tableExists(target_table):
        logger.info("Replacing load_date=%s in %s", load_date, target_table)
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"load_date = '{load_date}'")
            .saveAsTable(target_table)
        )
    else:
        logger.info("Creating %s", target_table)
        writer.mode("overwrite").partitionBy("load_date").saveAsTable(target_table)

    rows = spark.read.table(target_table).filter(F.col("load_date") == F.lit(load_date)).count()
    logger.info("%s rows in %s for load_date=%s", rows, target_table, load_date)
    return rows
