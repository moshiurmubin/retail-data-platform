# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: store sales (bronze.s3_sales -> silver.sales)
# MAGIC Parse the `product` JSON string, flatten it, cast types, quarantine bad rows,
# MAGIC deduplicate on customer + product + order_date, then SCD Type 1 merge.

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
from common_utils.bronze import read_bronze_for_date
from common_utils.config import load_config
from common_utils.data_quality import split_valid_invalid, write_quarantine
from common_utils.generic_functions import deduplicate_from_config
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger
from common_utils.scdfunctions import scd_type_1
from transformations.silver_sales import transform_sales

logger = get_logger("b2s_sales")

params = get_run_params(dbutils, "../config/config_b2s_sales.json")
config = load_config(params["path"], catalog=params["catalog"])

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "silver_sales", params["run_date"])

# COMMAND ----------

try:
    bronze_df = read_bronze_for_date(spark, config["source"]["source_table"], params["run_date"])

    transformed = transform_sales(bronze_df, config)

    valid, invalid = split_valid_invalid(transformed, config["quarantine"]["rules"])
    write_quarantine(spark, invalid, config["quarantine"]["table"])

    sales = deduplicate_from_config(valid, config)
    rows = sales.count()

    scd_type_1(spark, sales, config["target"]["target_table"], config["scd"]["key_columns"])
    run.success(target=config["target"]["target_table"], row_count=rows)
except Exception as e:
    run.failure(e)
    raise
