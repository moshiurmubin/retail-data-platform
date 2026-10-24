# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw volume -> Bronze Delta tables (ds2b = data source to bronze)
# MAGIC One config-driven notebook for all three sources (Class 11).
# MAGIC For each source: `read_raw()` the run's `load_date` folder, then `bronze_ingestor()`
# MAGIC adds audit columns (`last_update_ts`, `file_path`, `load_date`, `source_system`)
# MAGIC and replaces only that `load_date` partition - re-running the same day never duplicates.

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
from common_utils.bronze import read_raw, bronze_ingestor
from common_utils.config import load_config
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("ds2b")

params = get_run_params(dbutils, "../config/config_ds2b.json")
config = load_config(params["path"], catalog=params["catalog"])
run_date = params["run_date"]

# COMMAND ----------

failures = []

for source_name, source in config["sources"].items():
    run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], f"bronze_{source_name}", run_date)
    try:
        raw_path = f"{config['base_raw_path']}/{source['raw_folder']}/load_date={run_date}"

        df = read_raw(spark, raw_path, source["file_format"], source.get("read_options"))

        rows = bronze_ingestor(
            spark, df, source["target_table"], load_date=run_date, source_system=source["source_system"]
        )
        run.success(target=source["target_table"], row_count=rows)

    except Exception as e:
        run.failure(e)
        failures.append(f"{source_name}: {e}")

if failures:
    raise RuntimeError("Bronze failed for: " + " | ".join(failures))

logger.info("Bronze finished for all sources")
