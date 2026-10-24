from datetime import datetime, timezone

import pyspark.sql.functions as F

from common_utils.logging import get_logger

logger = get_logger("data_quality")


def _failed_result(check, table_name, total_records, failed_records, message, status=None):
    severity = check.get("severity", "error").lower()
    on_failure = check.get("on_failure", "fail").lower()

    if status is None:
        status = "PASS" if failed_records == 0 else "FAIL"

    if failed_records == 0:
        status = "PASS"
    elif on_failure == "warn" or severity == "warn":
        status = "WARN"

    return {
        "run_id": None,
        "run_timestamp": None,
        "layer": None,
        "table_name": table_name,
        "check_name": check["name"],
        "check_type": check["type"],
        "status": status,
        "total_records": int(total_records),
        "failed_records": int(failed_records),
        "severity": severity,
        "on_failure": on_failure,
        "message": message,
    }


def check_not_null(df, check):
    column = check["column"]
    total = df.count()
    failed = df.filter(F.col(column).isNull()).count()

    return total, failed, f"Column [{column}] contains {failed} null records"


def check_unique(df, check):
    columns = check.get("columns") or [check["column"]]
    total = df.count()

    duplicate_groups = (
        df.groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return (
        total,
        duplicate_groups,
        f"Duplicate groups for [{', '.join(columns)}]: {duplicate_groups}",
    )


def check_range(df, check):
    column = check["column"]
    total = df.count()

    condition = F.lit(False)

    if "min" in check:
        condition = condition | (F.col(column) < F.lit(check["min"]))

    if "max" in check:
        condition = condition | (F.col(column) > F.lit(check["max"]))

    failed = df.filter(condition).count()

    return total, failed, f"Column [{column}] has {failed} records outside the configured range"


def check_accepted_values(df, check):
    column = check["column"]
    values = check["values"]
    total = df.count()

    failed = (
        df.filter(
            F.col(column).isNotNull() & ~F.col(column).isin(values)
        )
        .count()
    )

    return total, failed, f"Column [{column}] has {failed} records outside accepted values"


def check_min_row_count(df, check):
    total = df.count()
    threshold = check["threshold"]
    failed = 0 if total >= threshold else 1

    return total, failed, f"Row count [{total}] must be at least [{threshold}]"


def check_expression(df, check):
    total = df.count()
    expression = check["expression"]

    failed = (
        df.filter(
            F.expr(f"NOT ({expression})")
        )
        .count()
    )

    return total, failed, f"Expression [{expression}] failed for {failed} records"


def check_referential_integrity(df, check, table_cache):
    total = df.count()

    parent_table = check["parent_table"]
    child_column = check["child_column"]
    parent_column = check.get("parent_column", child_column)

    parent_df = table_cache[parent_table]

    failed = (
        df.alias("child")
        .filter(F.col(f"child.{child_column}").isNotNull())
        .join(
            parent_df.select(
                F.col(parent_column).alias("__parent_key")
            ).distinct(),
            F.col(f"child.{child_column}") == F.col("__parent_key"),
            "left_anti",
        )
        .count()
    )

    return (
        total,
        failed,
        f"[{child_column}] has {failed} records without a matching key in [{parent_table}.{parent_column}]",
    )


def check_freshness(df, check):
    column = check["column"]
    total = df.count()
    max_value = df.select(F.max(F.col(column)).alias("max_value")).first()["max_value"]

    if max_value is None:
        return total, 1, f"Freshness column [{column}] has no non-null value"

    max_age_minutes = check["max_age_minutes"]
    current_ts = F.current_timestamp()

    failed = (
        df.select(F.max(F.col(column)).alias("max_value"))
        .filter(
            (current_ts.cast("long") - F.col("max_value").cast("timestamp").cast("long"))
            > (max_age_minutes * 60)
        )
        .count()
    )

    return (
        total,
        failed,
        f"Latest [{column}] value is [{max_value}], maximum age allowed is [{max_age_minutes}] minutes",
    )



def check_reconciliation(df, check, table_cache):
    total = df.count()

    source_table = check["source_table"]
    source_df = table_cache[source_table]

    source_agg = check.get("source_aggregation", "count")
    target_agg = check.get("target_aggregation", "count")
    source_column = check.get("source_column")
    target_column = check.get("target_column")
    tolerance = check.get("tolerance", 0)

    def aggregate(frame, aggregation, column=None):
        if aggregation == "count":
            return frame.count()
        if aggregation == "sum":
            return frame.select(F.sum(F.col(column)).alias("value")).first()["value"] or 0
        if aggregation == "avg":
            return frame.select(F.avg(F.col(column)).alias("value")).first()["value"] or 0
        raise ValueError(f"Unsupported reconciliation aggregation: {aggregation}")

    source_value = aggregate(source_df, source_agg, source_column)
    target_value = aggregate(df, target_agg, target_column)

    difference = abs(float(target_value) - float(source_value))
    failed = 0 if difference <= tolerance else 1

    return (
        total,
        failed,
        f"Source [{source_table}]={source_value}, target={target_value}, difference={difference}, tolerance={tolerance}",
    )

def run_check(df, check, table_name, table_cache):
    check_type = check["type"].lower()

    if check_type == "not_null":
        return check_not_null(df, check)

    if check_type == "unique":
        return check_unique(df, check)

    if check_type == "range":
        return check_range(df, check)

    if check_type == "accepted_values":
        return check_accepted_values(df, check)

    if check_type == "min_row_count":
        return check_min_row_count(df, check)

    if check_type == "expression":
        return check_expression(df, check)

    if check_type == "referential_integrity":
        return check_referential_integrity(df, check, table_cache)

    if check_type == "freshness":
        return check_freshness(df, check)

    if check_type == "reconciliation":
        return check_reconciliation(df, check, table_cache)

    raise ValueError(f"Unsupported data quality check type: {check_type}")


def run_data_quality(df, config, table_cache=None):
    table_cache = table_cache or {}
    table_name = config["table"]
    results = []

    logger.info(
        "[%s] Starting data quality checks",
        table_name,
    )

    for check in config.get("checks", []):
        logger.info(
            "[%s] Running DQ check [%s] [%s]",
            table_name,
            check["name"],
            check["type"],
        )

        total, failed, message = run_check(
            df,
            check,
            table_name,
            table_cache,
        )

        result = _failed_result(
            check,
            table_name,
            total,
            failed,
            message,
        )

        logger.info(
            "[%s] DQ check [%s] status=%s failed_records=%s",
            table_name,
            check["name"],
            result["status"],
            failed,
        )

        results.append(result)

    return results


def create_dq_audit_table(spark, audit_table):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            run_id STRING,
            run_timestamp TIMESTAMP,
            layer STRING,
            table_name STRING,
            check_name STRING,
            check_type STRING,
            status STRING,
            total_records BIGINT,
            failed_records BIGINT,
            severity STRING,
            on_failure STRING,
            message STRING
        )
        USING DELTA
    """)


def write_dq_results(spark, results, audit_table, run_id, layer):
    if not results:
        return

    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = []
    for result in results:
        rows.append(
            (
                run_id,
                timestamp,
                layer,
                result["table_name"],
                result["check_name"],
                result["check_type"],
                result["status"],
                result["total_records"],
                result["failed_records"],
                result["severity"],
                result["on_failure"],
                result["message"],
            )
        )

    # Build using explicit Spark SQL types through a temporary DataFrame schema.
    df = spark.createDataFrame(
        rows,
        [
            "run_id",
            "run_timestamp",
            "layer",
            "table_name",
            "check_name",
            "check_type",
            "status",
            "total_records",
            "failed_records",
            "severity",
            "on_failure",
            "message",
        ],
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(audit_table)
    )


def has_failures(results):
    return any(
        result["status"] == "FAIL"
        and result["on_failure"] == "fail"
        for result in results
    )


# ============================================================
# QUARANTINE
# ============================================================

def split_valid_invalid(df, rules):
    """
    Split a dataframe with row-level rules BEFORE it reaches a trusted table.

    rules : list of {"name": "customer_id_not_null", "condition": "customer_id IS NOT NULL"}
            condition = what a GOOD row looks like (SQL expression)

    returns (valid_df, invalid_df); invalid_df has dq_failed_rules (comma list)
    and dq_quarantined_at so someone can investigate it.
    """
    if not rules:
        return df, df.limit(0)

    failed_flags = [
        F.when(~F.coalesce(F.expr(rule["condition"]), F.lit(False)), F.lit(rule["name"]))
        for rule in rules
    ]

    flagged = df.withColumn("dq_failed_rules", F.concat_ws(",", *failed_flags))

    valid = flagged.filter(F.col("dq_failed_rules") == "").drop("dq_failed_rules")
    invalid = (
        flagged.filter(F.col("dq_failed_rules") != "")
        .withColumn("dq_quarantined_at", F.current_timestamp())
    )
    return valid, invalid


def write_quarantine(spark, invalid_df, quarantine_table):
    """Append quarantined rows (schema can grow over time). Returns the count written."""
    count = invalid_df.count()
    if count == 0:
        logger.info("No rows quarantined for %s", quarantine_table)
        return 0

    logger.warning("Quarantining %s rows into %s", count, quarantine_table)
    (
        invalid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table)
    )
    return count
