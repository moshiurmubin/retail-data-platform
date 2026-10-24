# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Data quality: Silver + Gold checks
# MAGIC Every table has its own config in `src/dq/config/config_dq_*.json`
# MAGIC (not_null, unique, range, accepted_values, min_row_count, expression,
# MAGIC referential_integrity, freshness, reconciliation).
# MAGIC Results go to `audit.dq_results` with a `run_id`. Any `on_failure: fail`
# MAGIC check that fails stops the job; `warn` is recorded only.

# COMMAND ----------

# Make the repo root importable (common_utils, transformations) whether this
# runs in a Git folder or from a deployed bundle.
import os, sys

_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "common_utils")):
    _root = os.path.dirname(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

# COMMAND ----------

import os
import uuid

from common_utils.config import load_config
from common_utils.data_quality import create_dq_audit_table, has_failures, run_data_quality, write_dq_results
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("run_data_quality")

params = get_run_params(dbutils, "../config/config_data_quality.json")
catalog = params["catalog"]
master = load_config(params["path"], catalog=catalog)

run_cfg = master["run"]
audit_table = run_cfg["audit_table"]
dq_run_id = params["job_run_id"] if params["job_run_id"] != "manual" else str(uuid.uuid4())

config_dir = os.path.dirname(params["path"])
configs = [load_config(os.path.join(config_dir, name), catalog=catalog) for name in master["configs"]]

run = RunLogger(spark, run_log_table(catalog), params["job_run_id"], "data_quality", params["run_date"])

# COMMAND ----------

# Every table a check reads (checked tables + parents of FK / reconciliation checks).
# No .cache(): serverless compute does not support it.
needed = set()
for cfg in configs:
    needed.add(cfg["table"])
    for check in cfg["checks"]:
        for key in ("parent_table", "source_table"):
            if key in check:
                needed.add(check[key])

table_cache = {name: spark.read.table(name) for name in needed}

# COMMAND ----------

_existing_cols = set(spark.read.table(audit_table).columns)
if "checked_at" in _existing_cols:
    spark.sql(f"ALTER TABLE {audit_table} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    spark.sql(f"ALTER TABLE {audit_table} RENAME COLUMN checked_at TO run_timestamp")
_to_add = [f"{c} {t}" for c, t in {"layer": "STRING", "total_records": "BIGINT", "on_failure": "STRING"}.items() if c not in _existing_cols]
if _to_add:
    spark.sql(f"ALTER TABLE {audit_table} ADD COLUMNS ({', '.join(_to_add)})")
create_dq_audit_table(spark, audit_table)

all_results = []
for cfg in configs:
    results = run_data_quality(table_cache[cfg["table"]], cfg, table_cache)
    for result in results:
        result["layer"] = cfg.get("layer", run_cfg.get("layer"))
    all_results.extend(results)

write_dq_results(spark, all_results, audit_table, dq_run_id, run_cfg.get("layer"))

# COMMAND ----------

results_df = spark.read.table(audit_table).filter(f"run_id = '{dq_run_id}'").orderBy("status", "table_name", "check_name")
display(results_df)
display(results_df.groupBy("status").count())

# COMMAND ----------

failed = [r["check_name"] + " @ " + r["table_name"] for r in all_results
          if r["status"] == "FAIL" and r["on_failure"] == "fail"]
warned = [r for r in all_results if r["status"] == "WARN"]
logger.info("DQ: %s checks, %s failed, %s warnings", len(all_results), len(failed), len(warned))

if run_cfg.get("fail_on_error", True) and has_failures(all_results):
    run.failure("Failed checks: " + ", ".join(failed))
    raise ValueError("Data Quality failed. Failed checks: " + ", ".join(failed))

run.success(target=audit_table, row_count=len(all_results))