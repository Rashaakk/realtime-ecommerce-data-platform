from pathlib import Path

BASE = Path("member4_analytics")
FILES = {}

FILES["sql/01_sales_summary.sql"] = '''-- Daily sales (canceled orders excluded)
SELECT
  DATE(order_timestamp)        AS order_date,
  ROUND(SUM(price), 2)         AS product_sales,
  ROUND(SUM(freight_value), 2) AS freight_total,
  ROUND(SUM(item_total), 2)    AS total_sales
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_date
ORDER BY order_date
'''

FILES["sql/02_order_volume.sql"] = '''-- Orders per day (canceled orders excluded)
SELECT
  DATE(order_timestamp)    AS order_date,
  COUNT(DISTINCT order_id) AS total_orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_date
ORDER BY order_date
'''

FILES["sql/03_avg_order_value.sql"] = '''-- Average order value (canceled orders excluded)
SELECT
  ROUND(SUM(item_total) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM {{TABLE}}
WHERE order_status != 'canceled'
'''

FILES["sql/04_top_products.sql"] = '''-- Top 10 products by revenue
SELECT
  product_id,
  category,
  COUNT(*)             AS units_sold,
  ROUND(SUM(price), 2) AS revenue
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY product_id, category
ORDER BY revenue DESC
LIMIT 10
'''

FILES["sql/05_top_categories.sql"] = '''-- Top 10 categories by revenue
SELECT
  category,
  ROUND(SUM(price), 2)     AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY category
ORDER BY revenue DESC
LIMIT 10
'''

FILES["sql/06_peak_hours.sql"] = '''-- Peak ordering hours
SELECT
  EXTRACT(HOUR FROM order_timestamp) AS order_hour,
  COUNT(DISTINCT order_id)           AS orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_hour
ORDER BY orders DESC
'''

FILES["dags/ecommerce_batch_dag.py"] = '''import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

log = logging.getLogger(__name__)

PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def _full_table():
    return f"`{PROJECT}.{DATASET}.{TABLE}`"


def check_source_data():
    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(f"SELECT COUNT(*) AS c FROM {_full_table()}").result())
    count = rows[0].c
    log.info("Source table has %s rows", count)
    if count == 0:
        raise ValueError("Source table is empty")


def refresh_table(sql_file, dest_name):
    client = bigquery.Client(project=PROJECT)
    sql = (SQL_DIR / sql_file).read_text().replace("{{TABLE}}", _full_table())
    dest = f"{PROJECT}.{DATASET}.{dest_name}"
    cfg = bigquery.QueryJobConfig(
        destination=dest,
        write_disposition="WRITE_TRUNCATE",
    )
    client.query(sql, job_config=cfg).result()
    log.info("Refreshed %s from %s", dest, sql_file)


def log_complete():
    log.info("Batch analytics run finished")


default_args = {
    "owner": "member4",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_batch_analytics",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    check = PythonOperator(task_id="check_source_data", python_callable=check_source_data)
    sales = PythonOperator(
        task_id="refresh_daily_sales",
        python_callable=refresh_table,
        op_args=["01_sales_summary.sql", "agg_daily_sales"],
    )
    categories = PythonOperator(
        task_id="refresh_top_categories",
        python_callable=refresh_table,
        op_args=["05_top_categories.sql", "agg_top_categories"],
    )
    peak = PythonOperator(
        task_id="refresh_peak_hours",
        python_callable=refresh_table,
        op_args=["06_peak_hours.sql", "agg_peak_hours"],
    )
    done = PythonOperator(task_id="log_complete", python_callable=log_complete)

    check >> [sales, categories, peak] >> done
'''

FILES["scripts/run_batch.py"] = '''"""Run the same batch steps as the Airflow DAG, without Airflow (works on Windows)."""
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
'''

FILES["tests/test_integration.py"] = '''import os

import pytest
from google.cloud import bigquery

PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")
FULL = f"`{PROJECT}.{DATASET}.{TABLE}`"

pytestmark = pytest.mark.skipif(not PROJECT, reason="BIGQUERY_PROJECT_ID not set")


def _scalar(sql):
    client = bigquery.Client(project=PROJECT)
    return list(client.query(sql).result())[0][0]


def test_table_has_rows():
    assert _scalar(f"SELECT COUNT(*) FROM {FULL}") > 0


def test_no_duplicate_order_items():
    dupes = _scalar(f"""
        SELECT COUNT(*) FROM (
          SELECT order_id, order_item_id FROM {FULL}
          GROUP BY 1, 2 HAVING COUNT(*) > 1)""")
    assert dupes == 0


def test_no_invalid_prices_or_totals():
    bad = _scalar(f"""
        SELECT COUNT(*) FROM {FULL}
        WHERE price < 0 OR freight_value < 0
           OR ABS(item_total - (price + freight_value)) > 0.01""")
    assert bad == 0
'''

for rel, content in FILES.items():
    path = BASE / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print("wrote", path)