# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: dim_order
# MAGIC One row per web-shop order: customer, timestamp, number of line items, `order_date_key`.

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
from transformations.gold import build_dim_order

params = get_run_params(dbutils, "../config/config_dim_order.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_dim_order", params["run_date"])

# COMMAND ----------

try:
    sales_orders = spark.read.table(config["source"]["source_table"])
    dim_order = build_dim_order(sales_orders, config)
    assert_not_empty_and_unique(dim_order, ["order_number"], "dim_order")
    rows = write_delta(spark, dim_order, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise
