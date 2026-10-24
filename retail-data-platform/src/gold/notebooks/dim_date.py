# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: dim_date
# MAGIC One row per calendar day (SEQUENCE + EXPLODE, Class 5), `date_key = yyyyMMdd`,
# MAGIC plus the Unknown member `-1`. Rebuilt fully each run (idempotent).

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
from common_utils.generic_functions import assert_not_empty_and_unique, write_delta
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger
from transformations.gold import build_dim_date

params = get_run_params(dbutils, "../config/config_dim_date.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_dim_date", params["run_date"])

# COMMAND ----------

try:
    dim_date = build_dim_date(spark, config)
    assert_not_empty_and_unique(dim_date, ["date_key"], "dim_date")
    rows = write_delta(spark, dim_date, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise
