# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: sales orders (bronze.cosmosdb_sales_orders -> 4 silver tables)
# MAGIC Semi-structured JSON (Class 14): `from_json()` with a schema, `explode()` the arrays,
# MAGIC select nested fields into columns. One document becomes:
# MAGIC
# MAGIC | Silver table | Grain | Business key |
# MAGIC | --- | --- | --- |
# MAGIC | sales_orders | one row per order | order_number |
# MAGIC | order_products | one row per order line | order_number + product_id |
# MAGIC | promotions | one row per promotion | order_number + promo_id + promo_product_id |
# MAGIC | clicked_items | one row per clicked product | order_number + product_id |
# MAGIC
# MAGIC Each table: quarantine -> deduplicate -> SCD Type 1 merge.

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
from transformations.silver_sales_orders import parse_sales_orders, TRANSFORMS

logger = get_logger("b2s_sales_orders")

params = get_run_params(dbutils, "../config/config_b2s_sales_orders.json")
config = load_config(params["path"], catalog=params["catalog"])

# COMMAND ----------

bronze_df = read_bronze_for_date(spark, config["source"]["source_table"], params["run_date"])
parsed = parse_sales_orders(bronze_df)

failures = []

for table_name, table_config in config["tables"].items():
    run = RunLogger(spark, run_log_table(params["catalog"]), params["job_run_id"], f"silver_{table_name}", params["run_date"])
    try:
        transformed = TRANSFORMS[table_name](parsed, table_config)

        valid, invalid = split_valid_invalid(transformed, table_config["quarantine"]["rules"])
        write_quarantine(spark, invalid, table_config["quarantine"]["table"])

        clean = deduplicate_from_config(valid, table_config)
        rows = clean.count()

        scd_type_1(spark, clean, table_config["target_table"], table_config["scd"]["key_columns"])
        run.success(target=table_config["target_table"], row_count=rows)

    except Exception as e:
        run.failure(e)
        failures.append(f"{table_name}: {e}")

if failures:
    raise RuntimeError("Silver sales_orders failed for: " + " | ".join(failures))
