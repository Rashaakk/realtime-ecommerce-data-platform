
from member2_processing.config import SPARK_OUTPUT_DIR

from member3_quality_storage.quality_validation import (
    split_valid_and_invalid,
)
from member3_quality_storage.parquet_storage import (
    save_quality_results,
)


def run_pipeline():
    """Validate Spark output and save quality results."""

    if not SPARK_OUTPUT_DIR.exists():
        raise FileNotFoundError(
            f"Spark output directory not found: {SPARK_OUTPUT_DIR}"
        )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("EcommerceDataQuality")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(SPARK_OUTPUT_DIR))

        valid_df, invalid_df = split_valid_and_invalid(df)

        print(f"Input records: {df.count()}")
        print(f"Valid records: {valid_df.count()}")
        print(f"Invalid records: {invalid_df.count()}")

        save_quality_results(valid_df, invalid_df)

        print("Data quality pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
