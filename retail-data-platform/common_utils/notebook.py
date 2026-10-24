"""
Small helpers every notebook uses to read its parameters the same way.
Job parameters (from resources/*.yml) override the defaults, so the same
notebook runs interactively and inside the job without changes.
"""
from datetime import date

from common_utils.config import get_param


def get_run_params(dbutils, default_config_path):
    return {
        "path": get_param(dbutils, "path", default_config_path),
        "catalog": get_param(dbutils, "catalog", "retaildataplatform"),
        "run_date": get_param(dbutils, "run_date", date.today().isoformat()),
        "job_run_id": get_param(dbutils, "job_run_id", "manual"),
        "secret_scope": get_param(dbutils, "secret_scope", "retail-platform-dev"),
    }


def run_log_table(catalog):
    return f"{catalog}.audit.pipeline_run_log"
