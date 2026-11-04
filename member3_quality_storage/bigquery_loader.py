
from google.cloud import bigquery

from member3_quality_storage.config import (
    GOOGLE_APPLICATION_CREDENTIALS,
    BIGQUERY_PROJECT_ID,
    BIGQUERY_DATASET,
    BIGQUERY_TABLE,
    VALIDATED_DIR,
)


def load_to_bigquery() -> None:
    """Load validated Parquet files into BigQuery."""

    if not BIGQUERY_PROJECT_ID:
        raise ValueError("Set BIGQUERY_PROJECT_ID environment variable.")

    if not GOOGLE_APPLICATION_CREDENTIALS:
        raise ValueError(
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )

    table_id = (
        f"{BIGQUERY_PROJECT_ID}."
        f"{BIGQUERY_DATASET}."
        f"{BIGQUERY_TABLE}"
    )

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    parquet_files = list(VALIDATED_DIR.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {VALIDATED_DIR}"
        )

    for parquet_file in parquet_files:
        with parquet_file.open("rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

        job.result()

        print(
            f"Loaded {parquet_file.name}: "
            f"{job.output_rows} rows"
        )
