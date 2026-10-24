from datetime import datetime, timezone

from common_utils.logging import get_logger

logger = get_logger("audit")

RUN_LOG_SCHEMA = (
    "job_run_id STRING, task_name STRING, run_date STRING, status STRING, "
    "target STRING, row_count BIGINT, started_at TIMESTAMP, finished_at TIMESTAMP, "
    "duration_seconds DOUBLE, error_message STRING"
)


def create_run_log_table(spark, run_log_table):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {run_log_table} ({RUN_LOG_SCHEMA}) USING DELTA")


def write_run_log(spark, run_log_table, job_run_id, task_name, run_date, status,
                  target=None, row_count=None, started_at=None, error_message=None):
    """Append one row per task run: what ran, on which data, how long, and whether it worked."""
    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    started_at = started_at or finished_at
    duration = (finished_at - started_at).total_seconds()

    row = [(
        str(job_run_id), task_name, str(run_date), status, target,
        None if row_count is None else int(row_count),
        started_at, finished_at, float(duration),
        None if error_message is None else str(error_message)[:2000],
    )]

    try:
        create_run_log_table(spark, run_log_table)
        spark.createDataFrame(row, RUN_LOG_SCHEMA).write.mode("append").saveAsTable(run_log_table)
    except Exception as exc:  # the run log must never hide the real error
        logger.warning("Could not write run log: %s", exc)


class RunLogger:
    """
    Usage in a notebook:

        run = RunLogger(spark, run_log_table, job_run_id, "silver_customers", run_date)
        try:
            ...
            run.success(target="catalog.silver.customers", row_count=n)
        except Exception as e:
            run.failure(e)
            raise
    """

    def __init__(self, spark, run_log_table, job_run_id, task_name, run_date):
        self.spark = spark
        self.run_log_table = run_log_table
        self.job_run_id = job_run_id
        self.task_name = task_name
        self.run_date = run_date
        self.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        logger.info("[%s] started (run_date=%s, job_run_id=%s)", task_name, run_date, job_run_id)

    def success(self, target=None, row_count=None):
        logger.info("[%s] succeeded: %s rows into %s", self.task_name, row_count, target)
        write_run_log(self.spark, self.run_log_table, self.job_run_id, self.task_name,
                      self.run_date, "SUCCESS", target, row_count, self.started_at)

    def failure(self, error):
        logger.error("[%s] failed: %s", self.task_name, error)
        write_run_log(self.spark, self.run_log_table, self.job_run_id, self.task_name,
                      self.run_date, "FAILED", None, None, self.started_at, error)
