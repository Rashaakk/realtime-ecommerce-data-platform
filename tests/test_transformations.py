
import pytest
from pyspark.sql import SparkSession

from member2_processing.transformations import (
    clean_order_events,
    transform_order_events,
    calculate_category_sales,
    calculate_hourly_orders
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("TransformationTests")
        .master("local[*]")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def sample_data(spark):
    data = [
        ("E1", "O1", 1, "C1", "P1", "S1",
         "delivered", "2026-01-10 10:30:00",
         "electronics", 100.0, 10.0, 110.0),

        ("E2", "O2", 1, "C2", "P2", "S2",
         "delivered", "2026-01-10 11:30:00",
         "clothing", 200.0, 20.0, 220.0),

        # Duplicate event
        ("E1", "O1", 1, "C1", "P1", "S1",
         "delivered", "2026-01-10 10:30:00",
         "electronics", 100.0, 10.0, 110.0)
    ]

    columns = [
        "event_id", "order_id", "order_item_id",
        "customer_id", "product_id", "seller_id",
        "order_status", "order_timestamp", "category",
        "price", "freight_value", "item_total"
    ]

    return spark.createDataFrame(data, columns)


def test_clean_order_events(spark):
    df = sample_data(spark)

    cleaned = clean_order_events(df)

    assert cleaned.count() == 2


def test_transform_order_events(spark):
    df = clean_order_events(sample_data(spark))

    transformed = transform_order_events(df)

    assert transformed.count() == 2

    row = transformed.filter(
        transformed.event_id == "E1"
    ).first()

    assert row.calculated_item_total == 110.0
    assert row.order_hour == 10


def test_category_sales(spark):
    df = transform_order_events(
        clean_order_events(sample_data(spark))
    )

    result = calculate_category_sales(df)

    assert result.count() == 2


def test_hourly_orders(spark):
    df = transform_order_events(
        clean_order_events(sample_data(spark))
    )

    result = calculate_hourly_orders(df)

    assert result.count() == 2
