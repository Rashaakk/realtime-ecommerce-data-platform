
import os
from pathlib import Path

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Input from Member 2 Spark processing
SPARK_OUTPUT_DIR = BASE_DIR / "data" / "processed" / "spark_output"

# Output directories for Member 3
QUALITY_OUTPUT_DIR = BASE_DIR / "data" / "quality"
VALIDATED_DIR = QUALITY_OUTPUT_DIR / "validated"
REJECTED_DIR = QUALITY_OUTPUT_DIR / "rejected"

# BigQuery configuration
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")
