# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold: vw_sales_all_channels
# MAGIC The single source of truth for revenue (the Class 8 problem): online and store
# MAGIC sales in one view with the same columns and the same dimensions.

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
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

params = get_run_params(dbutils, "../config/config_vw_sales_all_channels.json")
config = load_config(params["path"], catalog=params["catalog"])

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "gold_vw_sales_all_channels", params["run_date"])

# COMMAND ----------

try:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {config['view']}
        COMMENT 'Online + store sales with conformed dimensions. Use this for total revenue.'
        AS
        SELECT channel, CAST(order_number AS STRING) AS order_number, customer_id, product_id,
               order_date_key, qty, unit_price, total_amount
        FROM {config['online_fact']}
        UNION ALL
        SELECT channel, CAST(NULL AS STRING) AS order_number, customer_id, product_id,
               order_date_key, qty, unit_price, total_amount
        FROM {config['store_fact']}
    """)
    run.success(target=config["view"])
except Exception as e:
    run.failure(e)
    raise

# COMMAND ----------

# One revenue number per month and channel
display(spark.sql(f"""
    SELECT d.year, d.month, s.channel, ROUND(SUM(s.total_amount), 2) AS revenue
    FROM {config['view']} s
    JOIN {params['catalog']}.gold.dim_date d ON s.order_date_key = d.date_key
    GROUP BY d.year, d.month, s.channel
    ORDER BY d.year, d.month, s.channel
"""))