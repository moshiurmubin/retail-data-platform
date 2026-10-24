# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Governance (Unity Catalog)
# MAGIC Driven by `config_governance.json`, re-applied after every load:
# MAGIC 1. **Comments** on tables and key columns (data dictionary in the catalog)
# MAGIC 2. **Tags**: layer, source, PII classification on tables and columns
# MAGIC 3. **Secure view**: `vw_dim_customer_secure` masks names unless the reader is in `pii_readers`
# MAGIC 4. **Grants**: consumers get Gold only - never Bronze/Silver, and customers only through the masked view
# MAGIC
# MAGIC Lineage needs no code: Catalog Explorer -> a Gold table -> Lineage.

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

import importlib, common_utils.config
importlib.reload(common_utils.config)

from common_utils.config import load_config
from common_utils.logging import get_logger
from common_utils.notebook import get_run_params, run_log_table
from common_utils.audit import RunLogger

logger = get_logger("governance")

params = get_run_params(dbutils, "../config/config_governance.json")
config = load_config(params["path"], catalog=params["catalog"])

run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], "governance", params["run_date"])

errors = []


def sql(statement):
    """Run one governance statement; collect the error instead of stopping."""
    try:
        spark.sql(statement)
        logger.info("OK: %s", statement.split("\n")[0][:120])
    except Exception as e:
        logger.warning("FAILED: %s -> %s", statement.split("\n")[0][:120], e)
        errors.append(f"{statement[:80]}... -> {str(e)[:200]}")


def q(text):
    return text.replace("'", "\\'")


def tags(mapping):
    return ", ".join(f"'{k}' = '{v}'" for k, v in mapping.items())

# COMMAND ----------

# 1. Comments
for table, comment in config.get("table_comments", {}).items():
    sql(f"COMMENT ON TABLE {table} IS '{q(comment)}'")

for table, columns in config.get("column_comments", {}).items():
    for column, comment in columns.items():
        sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{q(comment)}'")

# 2. Tags
for table, tag_map in config.get("table_tags", {}).items():
    sql(f"ALTER TABLE {table} SET TAGS ({tags(tag_map)})")

for table, columns in config.get("column_tags", {}).items():
    for column, tag_map in columns.items():
        sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ({tags(tag_map)})")

# COMMAND ----------

# 3. Secure (masked) views
for view in config.get("secure_views", []):
    columns = spark.read.table(view["source"]).columns
    select_list = ",\n  ".join(
        f"CASE WHEN is_account_group_member('{view['pii_reader_group']}') THEN {c} ELSE '***' END AS {c}"
        if c in view["masked_columns"] else c
        for c in columns
    )
    sql(f"CREATE OR REPLACE VIEW {view['view']} AS SELECT\n  {select_list}\nFROM {view['source']}")

# 4. Grants
for grant in config.get("grants", []):
    privileges = ", ".join(grant["privileges"])
    sql(f"GRANT {privileges} ON {grant['securable_type']} {grant['securable']} TO `{grant['principal']}`")

# COMMAND ----------

if errors:
    logger.warning("%s governance statements failed:\n%s", len(errors), "\n".join(errors))
    if config.get("fail_on_error", False):
        run.failure(f"{len(errors)} governance statements failed")
        raise RuntimeError("Governance failed:\n" + "\n".join(errors))

run.success(target=params["catalog"], row_count=len(errors))

display(spark.sql(f"SHOW GRANTS ON SCHEMA {params['catalog']}.gold"))