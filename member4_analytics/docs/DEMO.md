\# Demo instructions (Member 4)



\## Prerequisites

\- Python 3.10+, Git, Google Cloud CLI

\- A Google Cloud project with a BigQuery dataset named `ecommerce`

\- Credentials: `gcloud auth application-default login`



\## Setup (Windows cmd)

```

git clone https://github.com/Rashaakk/realtime-ecommerce-data-platform.git

cd realtime-ecommerce-data-platform

git checkout member4-analytics

python -m venv .venv

.venv\\Scripts\\activate.bat

python -m pip install -r member4\_analytics\\requirements.txt

set BIGQUERY\_PROJECT\_ID=<your-project-id>

set GOOGLE\_APPLICATION\_CREDENTIALS=%APPDATA%\\gcloud\\application\_default\_credentials.json

```



\## Load data (once)

Full pipeline: Member 1 producer, Member 2 Spark job, Member 3 validation and loader.

For a quick demo, generate sample data and load it with Member 3's loader:

```

python member4\_analytics\\scripts\\generate\_sample\_data.py

python -c "from member3\_quality\_storage.bigquery\_loader import load\_to\_bigquery; load\_to\_bigquery()"

```

Run the loader only once: it uses WRITE\_APPEND, so a second run duplicates every row.



\## Run the batch job

```

python member4\_analytics\\scripts\\run\_batch.py

```

Refreshes `agg\_daily\_sales`, `agg\_top\_categories` and `agg\_peak\_hours`, with 3 retries and

logging to `member4\_analytics\\logs\\batch.log`. The Airflow equivalent is

`dags/ecommerce\_batch\_dag.py` (check\_source\_data, then three refresh tasks, then

log\_complete; retries=3; daily schedule).



\## Run the dashboard

```

python -m streamlit run member4\_analytics\\dashboard\\app.py

```

Open http://localhost:8501.



\## Run the tests

```

python -m pytest member4\_analytics\\tests -v

```

DAG tests are skipped unless Airflow is installed.



\## Expected results (sample data)

2,446 orders, 299,674.71 total sales, 122.52 average order value.



\## Screenshots

!\[Dashboard](screenshots/dashboard\_overview.png)

!\[Tests](screenshots/tests\_passing.png)

!\[Batch run](screenshots/batch\_run.png)

!\[BigQuery tables](screenshots/bigquery\_tables.png)

