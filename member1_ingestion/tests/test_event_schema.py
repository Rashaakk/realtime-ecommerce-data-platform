
import pytest

from member1_ingestion.event_schema import validate_event


@pytest.fixture
def valid_event():
    return {
        "event_id": "order123_1",
        "order_id": "order123",
        "order_item_id": 1,
        "customer_id": "customer456",
        "product_id": "product789",
        "seller_id": "seller321",
        "order_status": "delivered",
        "order_timestamp": "2018-01-15T10:30:00",
        "category": "electronics",
        "price": 249.99,
        "freight_value": 15.00,
        "item_total": 264.99,
    }


def test_valid_event_passes(valid_event):
    validate_event(valid_event)


def test_missing_field_fails(valid_event):
    valid_event.pop("order_id")

    with pytest.raises(ValueError, match="Missing required fields"):
        validate_event(valid_event)


def test_negative_price_fails(valid_event):
    valid_event["price"] = -1

    with pytest.raises(ValueError, match="cannot be negative"):
        validate_event(valid_event)


def test_invalid_timestamp_fails(valid_event):
    valid_event["order_timestamp"] = "not-a-date"

    with pytest.raises(ValueError, match="Invalid order_timestamp"):
        validate_event(valid_event)
