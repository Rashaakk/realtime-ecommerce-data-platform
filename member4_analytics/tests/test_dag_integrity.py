import os

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

os.environ.setdefault("BIGQUERY_PROJECT_ID", "test-project")


def test_dag_loads_without_errors():
    bag = DagBag("member4_analytics/dags", include_examples=False)
    assert not bag.import_errors


def test_dag_structure():
    bag = DagBag("member4_analytics/dags", include_examples=False)
    dag = bag.get_dag("ecommerce_batch_analytics")
    assert dag is not None
    assert dag.default_args["retries"] == 3
    assert {"check_source_data", "refresh_daily_sales",
            "refresh_top_categories", "refresh_peak_hours",
            "log_complete"} <= set(dag.task_ids)