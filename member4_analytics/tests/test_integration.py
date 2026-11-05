import os

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
