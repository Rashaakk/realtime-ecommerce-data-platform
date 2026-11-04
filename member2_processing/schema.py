
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# Schema for incoming e-commerce order events
ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("item_total", DoubleType(), True)
])
