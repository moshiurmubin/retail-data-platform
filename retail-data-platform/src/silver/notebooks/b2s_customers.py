# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver: customers (bronze.sqlserver_customers -> silver)
# MAGIC 1. Read this run's Bronze rows
# MAGIC 2. Clean (Class 13 rules) - `transformations/silver_customers.py`
# MAGIC 3. Quarantine rows that break row rules -> `silver.customers_quarantine`
# MAGIC 4. Deduplicate on the business key `customer_id`
# MAGIC 5. **SCD Type 1** -> `silver.customers` (current state)
# MAGIC 6. **SCD Type 2** -> `silver.customers_history` (every version, `is_current` flag)

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
from common_utils.scdfunctions import scd_type_1, scd_type_2
from transformations.silver_customers import transform_customers

logger = get_logger("b2s_customers")

params = get_run_params(dbutils, "../config/config_b2s_customers.json")
config = load_config(params["path"], catalog=params["catalog"])

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "silver_customers", params["run_date"])

# COMMAND ----------

try:
    bronze_df = read_bronze_for_date(spark, config["source"]["source_table"], params["run_date"])

    cleaned = transform_customers(bronze_df, config)

    valid, invalid = split_valid_invalid(cleaned, config["quarantine"]["rules"])
    quarantined = write_quarantine(spark, invalid, config["quarantine"]["table"])

    customers = deduplicate_from_config(valid, config)
    rows = customers.count()
    logger.info("%s clean customers, %s quarantined", rows, quarantined)

    for scd in config["scd"]:
        if scd["type"] == 1:
            scd_type_1(spark, customers, scd["target_table"], scd["key_columns"])
        elif scd["type"] == 2:
            scd_type_2(spark, customers, scd["target_table"], scd["key_columns"], scd["tracked_columns"])
        else:
            raise ValueError(f"Unsupported SCD type {scd['type']}")

    run.success(target=config["scd"][0]["target_table"], row_count=rows)
except Exception as e:
    run.failure(e)
    raise

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, first_name, last_name, city, is_current, effective_from, effective_to
    FROM {config['scd'][1]['target_table']}
    ORDER BY customer_id, effective_from
    LIMIT 20
"""))