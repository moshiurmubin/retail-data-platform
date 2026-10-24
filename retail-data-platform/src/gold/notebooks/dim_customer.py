# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold: dim_customer
# MAGIC From `silver.customers` (current state): names, `full_name`, `customer_type`,
# MAGIC `created_date`, `last_updated`, plus the Unknown member `customer_id = -1`.

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
from transformations.gold import build_dim_customer

params = get_run_params(dbutils, "../config/config_dim_customer.json")
config = load_config(params["path"], catalog=params["catalog"])
target = config["target"]["target_table"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_dim_customer", params["run_date"])

# COMMAND ----------

try:
    customers = spark.read.table(config["source"]["source_table"])
    dim_customer = build_dim_customer(spark, customers, config)
    assert_not_empty_and_unique(dim_customer, ["customer_id"], "dim_customer")
    rows = write_delta(spark, dim_customer, target)
    run.success(target=target, row_count=rows)
except Exception as e:
    run.failure(e)
    raise

# COMMAND ----------

# Does the scope exist?
print([s.name for s in dbutils.secrets.listScopes()])

# Which keys are inside it?
print([s.key for s in dbutils.secrets.list("retail-platform-dev")])