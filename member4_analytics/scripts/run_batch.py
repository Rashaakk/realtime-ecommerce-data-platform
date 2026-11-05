"""Run the same batch steps as the Airflow DAG, without Airflow (works on Windows)."""
import logging
import os
import sys
import time
from pathlib import Path

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "batch.log")],
)
log = logging.getLogger("batch")

PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")
if not PROJECT:
    sys.exit("Set BIGQUERY_PROJECT_ID first.")

client = bigquery.Client(project=PROJECT)
full_table = f"`{PROJECT}.{DATASET}.{TABLE}`"

STEPS = [
    ("01_sales_summary.sql", "agg_daily_sales"),
    ("05_top_categories.sql", "agg_top_categories"),
    ("06_peak_hours.sql", "agg_peak_hours"),
]


def with_retries(fn, attempts=3, delay=5):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %s/%s failed: %s", i, attempts, exc)
            if i == attempts:
                raise
            time.sleep(delay)


def main():
    log.info("Task check_source_data started")
    rows = list(client.query(f"SELECT COUNT(*) AS c FROM {full_table}").result())
    if rows[0].c == 0:
        raise ValueError("Source table is empty")
    log.info("Source rows: %s", rows[0].c)

    for sql_file, dest_name in STEPS:
        def run(sql_file=sql_file, dest_name=dest_name):
            sql = (ROOT / "sql" / sql_file).read_text().replace("{{TABLE}}", full_table)
            cfg = bigquery.QueryJobConfig(
                destination=f"{PROJECT}.{DATASET}.{dest_name}",
                write_disposition="WRITE_TRUNCATE",
            )
            client.query(sql, job_config=cfg).result()
            log.info("Refreshed %s from %s", dest_name, sql_file)
        with_retries(run)

    log.info("Batch finished successfully")


if __name__ == "__main__":
    main()
