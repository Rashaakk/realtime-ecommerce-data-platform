import logging
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
