\# Member 4: Orchestration, Analytics and Dashboard



BigQuery, then batch aggregation (Airflow DAG), then a Streamlit dashboard.



| Folder | Contents |

|---|---|

| `sql/` | Sales, order volume, average order value, top products, top categories, peak hours |

| `dags/` | Airflow DAG `ecommerce\_batch\_analytics` (dependencies, retries, logging) |

| `scripts/` | `run\_batch.py` (Windows-friendly batch runner), sample data generator |

| `dashboard/` | Streamlit dashboard |

| `tests/` | SQL file tests, DAG integrity test, BigQuery integration tests |

| `docs/` | `DEMO.md`, `ANALYSIS.md`, screenshots |



SQL files use a `{{TABLE}}` placeholder that the DAG and batch runner replace with

`<BIGQUERY\_PROJECT\_ID>.<BIGQUERY\_DATASET>.<BIGQUERY\_TABLE>`.



See `docs/DEMO.md` for run instructions and `docs/ANALYSIS.md` for findings.

