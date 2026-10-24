# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: fact_sales (online)
# MAGIC **Grain:** one row per order line (`order_number` + `product_id`).
# MAGIC `order_products` + `sales_orders` (customer, date) + `promotions` (discount).
# MAGIC `total_amount = qty * unit_price * (1 - discount)`.
# MAGIC Keys not found in a dimension point to its Unknown member.

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
from transformations.gold import build_fact_sales

params = get_run_params(dbutils, "../config/config_fact_sales.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]
src, dims = config["source"], config["dimensions"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_fact_sales", params["run_date"])

# COMMAND ----------

try:
    fact = build_fact_sales(
        spark.read.table(src["sales_orders"]),
        spark.read.table(src["order_products"]),
        spark.read.table(src["promotions"]),
        spark.read.table(dims["dim_customer"]),
        spark.read.table(dims["dim_product"]),
        spark.read.table(dims["dim_date"]),
        config,
    )
    assert_not_empty_and_unique(fact, ["order_number", "product_id"], "fact_sales")
    rows = write_delta(spark, fact, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise
