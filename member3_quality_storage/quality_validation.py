
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "event_id",
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_status",
    "order_timestamp",
    "category",
    "price",
    "freight_value",
    "item_total",
]


def validate_columns(df: DataFrame) -> None:
    """Ensure that all expected columns exist."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )


def split_valid_and_invalid(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split incoming records into valid and invalid DataFrames.

    Invalid records are retained for investigation.
    """
    validate_columns(df)

    checks = (
        F.col("event_id").isNotNull()
        & (F.trim(F.col("event_id")) != "")
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("seller_id").isNotNull()
        & F.col("order_status").isNotNull()
        & F.col("order_timestamp").isNotNull()
        & F.col("category").isNotNull()
        & F.col("order_item_id").isNotNull()
        & (F.col("order_item_id") > 0)
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
        & F.col("freight_value").isNotNull()
        & (F.col("freight_value") >= 0)
        & F.col("item_total").isNotNull()
        & (
            F.abs(
                F.col("item_total")
                - (F.col("price") + F.col("freight_value"))
            ) <= F.lit(0.01)
        )
    )

    valid_df = df.filter(checks)
    invalid_df = df.filter(~checks)

    return valid_df, invalid_df
