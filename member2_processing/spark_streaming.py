
import os
import sys
from pathlib import Path

# Configure Spark workers to use the active virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    lit,
    coalesce
)

from member2_processing.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SPARK_OUTPUT_DIR,
    QUARANTINE_DIR,
    CHECKPOINT_DIR,
    MAX_OFFSETS_PER_TRIGGER
)

from member2_processing.schema import ORDER_EVENT_SCHEMA

from member2_processing.transformations import (
    clean_order_events,
    transform_order_events,
    calculate_category_sales,
    calculate_hourly_orders
)


# Convert Windows paths into file URIs for Spark
def to_file_uri(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve().as_uri()


# Create Spark session
def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("EcommerceOrderStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
        )
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# Process every Kafka micro-batch
def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No records received")
        return

    print(f"\nProcessing batch: {batch_id}")

    # Parse Kafka JSON messages
    expanded_df = (
        batch_df
        .withColumn(
            "data",
            from_json(col("raw_json"), ORDER_EVENT_SCHEMA)
        )
        .withColumn(
            "is_parsed",
            col("data").isNotNull()
        )
        .select(
            "raw_json",
            "is_parsed",
            "data.*"
        )
    )

    # Identify valid records
    valid_condition = (
        col("is_parsed")
        & col("event_id").isNotNull()
        & col("order_id").isNotNull()
        & col("order_timestamp").isNotNull()
        & to_timestamp(col("order_timestamp")).isNotNull()
        & col("price").isNotNull()
        & col("freight_value").isNotNull()
        & (col("price") >= 0)
        & (col("freight_value") >= 0)
    )

    valid_condition = coalesce(valid_condition, lit(False))

    # Send invalid records to quarantine
    invalid_df = (
        expanded_df
        .filter(~valid_condition)
        .select(
            "raw_json",
            lit(batch_id).alias("batch_id"),
            current_timestamp().alias("quarantined_at")
        )
    )

    invalid_count = invalid_df.count()

    if invalid_count > 0:
        invalid_df.write \
            .mode("append") \
            .parquet(to_file_uri(QUARANTINE_DIR))

    # Extract valid records
    valid_df = (
        expanded_df
        .filter(valid_condition)
        .drop("raw_json", "is_parsed")
    )

    # Clean and transform
    cleaned_df = clean_order_events(valid_df)

    transformed_df = transform_order_events(cleaned_df)

    processed_count = transformed_df.count()

    # Save processed records
    if processed_count > 0:

        transformed_df.write \
            .mode("append") \
            .parquet(to_file_uri(SPARK_OUTPUT_DIR))

        # Category-level sales
        category_df = (
            calculate_category_sales(transformed_df)
            .withColumn("batch_id", lit(batch_id))
        )

        category_df.write \
            .mode("append") \
            .parquet(
                to_file_uri(
                    Path(SPARK_OUTPUT_DIR).parent / "category_sales"
                )
            )

        # Hourly order analytics
        hourly_df = (
            calculate_hourly_orders(transformed_df)
            .withColumn("batch_id", lit(batch_id))
        )

        hourly_df.write \
            .mode("append") \
            .parquet(
                to_file_uri(
                    Path(SPARK_OUTPUT_DIR).parent / "hourly_orders"
                )
            )

    print(f"Valid records processed: {processed_count}")
    print(f"Invalid records quarantined: {invalid_count}")


# Start the streaming application
def main():

    spark = create_spark_session()

    print("Spark Structured Streaming started!")
    print("Kafka broker:", KAFKA_BOOTSTRAP_SERVERS)
    print("Kafka topic:", KAFKA_TOPIC)

    # Read messages from Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    # Convert Kafka value from binary to string
    json_stream = kafka_stream.selectExpr(
        "CAST(value AS STRING) AS raw_json"
    )

    # Start processing with foreachBatch
    query = (
        json_stream.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            to_file_uri(CHECKPOINT_DIR)
        )
        .start()
    )

    print("Listening for Kafka events...")

    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping Spark streaming application...")

        query.stop()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
