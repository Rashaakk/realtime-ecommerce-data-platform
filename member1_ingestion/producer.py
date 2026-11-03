
import argparse
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from member1_ingestion.data_loader import (
    load_order_events,
    OUTPUT_DIR,
)
from member1_ingestion.event_schema import validate_event
from member1_ingestion.kafka_config import (
    BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    EVENT_DELAY_SECONDS,
    BATCH_LOG_INTERVAL,
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Publish e-commerce order-item events to Kafka."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum events to publish; use 0 for all events.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=EVENT_DELAY_SECONDS,
        help="Seconds to wait between events.",
    )
    args = parser.parse_args()

    if args.limit < 0 or args.delay < 0:
        parser.error("--limit and --delay cannot be negative")

    events = load_order_events()

    if args.limit:
        events = events.head(args.limit)

    producer = create_producer()
    sent = 0

    try:
        for event in events.to_dict(orient="records"):
            # Convert Pandas/NumPy values to standard Python types.
            event["order_item_id"] = int(event["order_item_id"])

            for field in ("price", "freight_value", "item_total"):
                event[field] = float(event[field])

            validate_event(event)

            future = producer.send(
                KAFKA_TOPIC,
                key=event["event_id"],
                value=event,
            )

            # Wait for broker acknowledgement.
            metadata = future.get(timeout=30)
            sent += 1

            if sent % BATCH_LOG_INTERVAL == 0 or sent == 1:
                print(
                    f"Published {sent} events | "
                    f"topic={metadata.topic} | "
                    f"partition={metadata.partition} | "
                    f"offset={metadata.offset}"
                )

            if args.delay:
                time.sleep(args.delay)

        producer.flush()
        print(f"Completed successfully: {sent} events published.")

    except (KafkaError, ValueError, OSError) as exc:
        print(f"Producer stopped due to an error: {exc}")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    main()