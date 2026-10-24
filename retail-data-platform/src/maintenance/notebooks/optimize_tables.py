# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance: OPTIMIZE + VACUUM (weekly job)
# MAGIC `OPTIMIZE` compacts the many small files that daily loads and MERGEs create, so reads stay fast.
# MAGIC `VACUUM` deletes data files older than the retention window that no table version uses any more
# MAGIC (time travel still works inside the window).

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
from common_utils.config import load_config
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("maintenance")

params = get_run_params(dbutils, "../config/config_maintenance.json")
config = load_config(params["path"], catalog=params["catalog"])
retention = int(config.get("vacuum_retention_hours", 168))

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "maintenance", params["run_date"])

# COMMAND ----------

done = 0
for table in config["optimize_tables"]:
    if not spark.catalog.tableExists(table):
        logger.warning("Skipping %s (does not exist yet)", table)
        continue
    spark.sql(f"OPTIMIZE {table}")
    spark.sql(f"VACUUM {table} RETAIN {retention} HOURS")
    logger.info("Optimized and vacuumed %s", table)
    done += 1

run.success(row_count=done)
