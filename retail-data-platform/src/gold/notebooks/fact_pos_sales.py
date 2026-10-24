# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: fact_pos_sales (physical stores)
# MAGIC **Grain:** one row per customer + product + order date, from `silver.sales` (S3 / POS).
# MAGIC Shares the conformed dimensions with `fact_sales`, so online and store sales
# MAGIC can be compared by customer, product and date.

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
from common_utils.generic_functions import write_delta
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger
from transformations.gold import build_fact_pos_sales

params = get_run_params(dbutils, "../config/config_fact_pos_sales.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]
dims = config["dimensions"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_fact_pos_sales", params["run_date"])

# COMMAND ----------

try:
    fact = build_fact_pos_sales(
        spark.read.table(config["source"]["sales"]),
        spark.read.table(dims["dim_customer"]),
        spark.read.table(dims["dim_product"]),
        spark.read.table(dims["dim_date"]),
        config,
    )
    rows = fact.count()
    if rows == 0:
        raise ValueError("fact_pos_sales has zero rows")
    rows = write_delta(spark, fact, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise
