# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion: Amazon S3 (store / POS sales) -> raw volume
# MAGIC Reads `sales.parquet` from S3 and lands it **as-is** as Parquet in
# MAGIC `raw_data/s3_sales/load_date=<run_date>/`.

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
from common_utils.ingestor import read_s3_parquet, write_raw
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("s3_ingestion")

params = get_run_params(dbutils, "../config/config_s3.json")
config = load_config(params["path"], catalog=params["catalog"])
source, target = config["source"], config["target"]

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "ingest_s3", params["run_date"])

# COMMAND ----------

try:
    access_key = dbutils.secrets.get(scope=params["secret_scope"], key=source["access_key_secret"])
    secret_key = dbutils.secrets.get(scope=params["secret_scope"], key=source["secret_key_secret"])

    df = read_s3_parquet(spark, access_key, secret_key, source["path"])
    rows = df.count()

    if rows == 0:
        logger.warning("S3 file has 0 rows")

    target_path = f"{target['base_path']}/{target['folder']}/load_date={params['run_date']}"
    write_raw(df, target_path, target["file_format"], target["mode"], config.get("write_options"))

    run.success(target=target_path, row_count=rows)
except Exception as e:
    run.failure(e)
    raise
