import os
from pathlib import Path

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "ecommerce_orders"
)

# Spark output directories
PROCESSED_DIR = BASE_DIR / "data" / "processed"

SPARK_OUTPUT_DIR = PROCESSED_DIR / "spark_output"

QUARANTINE_DIR = PROCESSED_DIR / "quarantine"

CHECKPOINT_DIR = PROCESSED_DIR / "checkpoints"

# Spark processing configuration
MAX_OFFSETS_PER_TRIGGER = 1000