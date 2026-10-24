import pyspark.sql.functions as F
from delta.tables import DeltaTable

from common_utils.logging import get_logger

logger = get_logger("scdfunctions")

SCD2_COLUMNS = ["record_hash", "effective_from", "effective_to", "is_current"]


def _check_keys(source_df, key_columns):
    missing = [c for c in key_columns if c not in source_df.columns]
    if missing:
        raise ValueError(f"Missing key columns in source DataFrame: {missing}")
    if not key_columns:
        raise ValueError("key_columns must not be empty")


def scd_type_1(spark, source_df, target_table, key_columns):
    """
    SCD Type 1: existing rows are updated, new rows are inserted, no history.

    First run (table missing) creates the table.
    Source must already be unique on key_columns.

    returns "created" or "merged"
    """
    _check_keys(source_df, key_columns)

    if not spark.catalog.tableExists(target_table):
        logger.info("SCD1: %s does not exist, creating it", target_table)
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return "created"

    condition = " AND ".join(f"target.`{c}` <=> source.`{c}`" for c in key_columns)
    logger.info("SCD1 merge into %s on %s", target_table, condition)

    (
        DeltaTable.forName(spark, target_table).alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return "merged"


def add_record_hash(df, tracked_columns):
    """sha2 hash of the tracked columns; NULL is encoded so it differs from ''."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked_columns]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *parts), 256))


def scd_type_2(spark, source_df, target_table, key_columns, tracked_columns, effective_ts=None):
    """
    SCD Type 2: keep full history.

    - new key                    -> insert as current row
    - key exists, tracked change -> close current row (effective_to, is_current=false)
                                    and insert the new version as current
    - key exists, no change      -> nothing

    Adds columns: record_hash, effective_from, effective_to, is_current.
    Source must already be unique on key_columns.

    returns "created" or "merged"
    """
    _check_keys(source_df, key_columns)

    ts = F.current_timestamp() if effective_ts is None else F.to_timestamp(F.lit(effective_ts))

    source = add_record_hash(source_df, tracked_columns).withColumn("effective_from", ts)

    if not spark.catalog.tableExists(target_table):
        logger.info("SCD2: %s does not exist, creating it", target_table)
        (
            source.withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").mode("overwrite").saveAsTable(target_table)
        )
        return "created"

    target = DeltaTable.forName(spark, target_table)
    current = spark.read.table(target_table).filter("is_current = true")

    # Rows whose tracked values changed need a SECOND copy: one closes the
    # old version (matched on the key), one is inserted as the new version
    # (merge key set to NULL so it can never match).
    changed = (
        source.alias("s")
        .join(current.alias("t"), [F.col(f"s.{c}") == F.col(f"t.{c}") for c in key_columns])
        .filter(F.col("s.record_hash") != F.col("t.record_hash"))
        .select("s.*")
    )

    staged = source
    for c in key_columns:
        staged = staged.withColumn(f"__mk_{c}", F.col(c))

    inserts = changed
    for c in key_columns:
        inserts = inserts.withColumn(f"__mk_{c}", F.lit(None).cast(source.schema[c].dataType))

    staged = staged.unionByName(inserts)

    condition = " AND ".join(f"t.`{c}` = s.`__mk_{c}`" for c in key_columns) + " AND t.is_current = true"

    insert_values = {c: f"s.`{c}`" for c in source.columns}
    insert_values["effective_to"] = "CAST(NULL AS TIMESTAMP)"
    insert_values["is_current"] = "true"

    logger.info("SCD2 merge into %s on %s", target_table, condition)

    (
        target.alias("t")
        .merge(staged.alias("s"), condition)
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={"is_current": "false", "effective_to": "s.effective_from"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    return "merged"
