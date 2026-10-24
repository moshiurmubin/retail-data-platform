# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the Unity Catalog objects the platform needs: catalog, the four
# MAGIC schemas (bronze, silver, gold, audit), the raw landing volume and the run-log table.
# MAGIC Safe to run again: everything is `IF NOT EXISTS`.

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
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import create_run_log_table

logger = get_logger("setup")

params = get_run_params(dbutils, "../config/config_setup.json")
catalog = params["catalog"]
config = load_config(params["path"], catalog=catalog)

# COMMAND ----------

# 1. Catalog (on some workspaces only an admin can create one - then create it in the UI once)
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    logger.info("Catalog %s ready", catalog)
except Exception as e:
    if catalog not in [r[0] for r in spark.sql("SHOW CATALOGS").collect()]:
        raise RuntimeError(
            f"Could not create catalog {catalog}. Create it in Catalog Explorer, then re-run. Error: {e}"
        )
    logger.warning("CREATE CATALOG not allowed, but %s already exists", catalog)

# 2. Schemas
for schema, comment in config["schemas"].items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{comment}'")
    logger.info("Schema %s.%s ready", catalog, schema)

# 3. Raw landing volume
volume = config["volume"]
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {catalog}.{volume['schema']}.{volume['name']} "
    f"COMMENT '{volume['comment']}'"
)
logger.info("Volume /Volumes/%s/%s/%s ready", catalog, volume["schema"], volume["name"])

# 4. Pipeline run log
create_run_log_table(spark, run_log_table(catalog))
logger.info("Run log table %s ready", run_log_table(catalog))

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
