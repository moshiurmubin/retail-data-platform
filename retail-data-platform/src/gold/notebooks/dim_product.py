# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold: dim_product
# MAGIC Conformed product dimension: products from online order lines **and** store sales,
# MAGIC category from store sales ("Unknown" when missing), plus the Unknown member `'UNKNOWN'`.

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
from transformations.gold import build_dim_product

params = get_run_params(dbutils, "../config/config_dim_product.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_dim_product", params["run_date"])

# COMMAND ----------

try:
    order_products = spark.read.table(config["source"]["order_products_table"])
    sales = spark.read.table(config["source"]["sales_table"])

    dim_product = build_dim_product(spark, order_products, sales, config)
    assert_not_empty_and_unique(dim_product, ["product_id"], "dim_product")
    rows = write_delta(spark, dim_product, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise