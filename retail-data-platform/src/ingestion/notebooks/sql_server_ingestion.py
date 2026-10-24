# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingestion: SQL Server (CRM customers) -> raw volume
# MAGIC Reads `retail.customers` over JDBC and lands it **as-is** as CSV in
# MAGIC `raw_data/sqlserver_customers/load_date=<run_date>/`. No business transformation here.

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
from common_utils.ingestor import read_jdbc, write_raw
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("sql_server_ingestion")

params = get_run_params(dbutils, "../config/config_sqlserver.json")
config = load_config(params["path"], catalog=params["catalog"])
source, target = config["source"], config["target"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "ingest_sqlserver", params["run_date"])

# COMMAND ----------

try:
    # Credentials come from the secret scope - the config only holds the key names
    user = dbutils.secrets.get(scope=params["secret_scope"], key=source["user_secret"])
    password = dbutils.secrets.get(scope=params["secret_scope"], key=source["password_secret"])

    df = read_jdbc(spark, source["url"], source["dbtable"], user, password, source["driver"])
    rows = df.count()
    logger.info("Read %s rows from %s", rows, source["dbtable"])

    if rows == 0:
        logger.warning("Source returned 0 rows")

    target_path = f"{target['base_path']}/{target['folder']}/load_date={params['run_date']}"
    write_raw(df, target_path, target["file_format"], target["mode"], config.get("write_options"))

    run.success(target=target_path, row_count=rows)
except Exception as e:
    run.failure(e)
    raise