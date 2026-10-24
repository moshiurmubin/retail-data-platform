# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingestion: Cosmos DB (web-shop orders) -> raw volume
# MAGIC Reads every document of `retail.sales_orders` through the MongoDB API (PyMongo)
# MAGIC and lands it **as-is** as JSON in `raw_data/cosmosdb_sales_orders/load_date=<run_date>/`.
# MAGIC Nested arrays stay nested; Silver flattens them.
# MAGIC
# MAGIC Needs `pymongo` (job environment installs it; interactively: Environment panel -> add `pymongo==4.10.1`).

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
from common_utils.ingestor import read_cosmosdb_json, write_raw
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("cosmosdb_ingestion")

params = get_run_params(dbutils, "../config/config_cosmosdb.json")
config = load_config(params["path"], catalog=params["catalog"])
source, target = config["source"], config["target"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "ingest_cosmosdb", params["run_date"])

# COMMAND ----------

# MAGIC %pip install pymongo==4.10.1
# MAGIC
# MAGIC try:
# MAGIC     connection_string = dbutils.secrets.get(scope=params["secret_scope"], key=source["connection_string_secret"])
# MAGIC
# MAGIC     df = read_cosmosdb_json(
# MAGIC         spark, connection_string, source["database_name"], source["collection_name"], source["column_name"]
# MAGIC     )
# MAGIC     rows = df.count()
# MAGIC
# MAGIC     if rows == 0:
# MAGIC         logger.warning("Cosmos DB returned 0 documents")
# MAGIC
# MAGIC     target_path = f"{target['base_path']}/{target['folder']}/load_date={params['run_date']}"
# MAGIC     write_raw(df, target_path, target["file_format"], target["mode"], config.get("write_options"))
# MAGIC
# MAGIC     run.success(target=target_path, row_count=rows)
# MAGIC except Exception as e:
# MAGIC     run.failure(e)
# MAGIC     raise