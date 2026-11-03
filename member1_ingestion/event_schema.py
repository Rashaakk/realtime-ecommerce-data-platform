
from datetime import datetime

REQUIRED_FIELDS = {
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
}


def validate_event(event: dict) -> None:
    """Raise ValueError if an event violates the agreed schema."""

    missing = REQUIRED_FIELDS - event.keys()

    if missing:
        raise ValueError(
            f"Missing required fields: {sorted(missing)}"
        )

    for field in (
        "event_id",
        "order_id",
        "customer_id",
        "product_id",
        "seller_id",
    ):
        if event[field] is None or not str(event[field]).strip():
            raise ValueError(f"{field} must not be empty")

    if not isinstance(event["order_item_id"], int):
        raise ValueError("order_item_id must be an integer")

    for field in ("price", "freight_value", "item_total"):
        value = event[field]

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field} must be numeric")

        if value < 0:
            raise ValueError(f"{field} cannot be negative")

    if abs(
        event["item_total"]
        - (event["price"] + event["freight_value"])
    ) > 0.01:
        raise ValueError(
            "item_total must equal price + freight_value"
        )

    try:
        datetime.fromisoformat(event["order_timestamp"])
    except (TypeError, ValueError):
        raise ValueError("Invalid order_timestamp format")