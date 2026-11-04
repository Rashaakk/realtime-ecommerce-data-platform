
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    round as spark_round
)


def clean_order_events(df: DataFrame) -> DataFrame:
    """Clean and validate incoming order events."""

    cleaned_df = (
        df
        .filter(col("event_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("order_timestamp").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("freight_value").isNotNull())
        .filter(col("price") >= 0)
        .filter(col("freight_value") >= 0)
        .dropDuplicates(["event_id"])
    )

    return cleaned_df


def transform_order_events(df: DataFrame) -> DataFrame:
    """Parse timestamps and derive useful columns."""

    transformed_df = (
        df
        .withColumn(
            "order_timestamp",
            to_timestamp(col("order_timestamp"))
        )
        .filter(col("order_timestamp").isNotNull())
        .withColumn(
            "order_date",
            to_date(col("order_timestamp"))
        )
        .withColumn(
            "order_hour",
            hour(col("order_timestamp"))
        )
        .withColumn(
            "calculated_item_total",
            spark_round(
                col("price") + col("freight_value"), 2
            )
        )
    )

    return transformed_df


def calculate_category_sales(df: DataFrame) -> DataFrame:
    """Calculate sales metrics for each product category."""

    from pyspark.sql.functions import sum as spark_sum, count

    return (
        df.groupBy("category")
        .agg(
            spark_sum("calculated_item_total").alias("total_sales"),
            count("event_id").alias("total_items")
        )
    )


def calculate_hourly_orders(df: DataFrame) -> DataFrame:
    """Calculate order metrics by date and hour."""

    from pyspark.sql.functions import count, sum as spark_sum

    return (
        df.groupBy("order_date", "order_hour")
        .agg(
            count("event_id").alias("total_items"),
            spark_sum("calculated_item_total").alias("total_sales")
        )
        .orderBy("order_date", "order_hour")
    )
