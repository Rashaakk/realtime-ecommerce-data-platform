
import os

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ecommerce_orders",
)

EVENT_DELAY_SECONDS = float(
    os.getenv("EVENT_DELAY_SECONDS", "0.2")
)

BATCH_LOG_INTERVAL = 100