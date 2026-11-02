
import pandas as pd

from member1_ingestion.data_loader import load_order_events


def test_loader_creates_order_item_events(tmp_path):
    pd.DataFrame([
        {
            "order_id": "o1",
            "customer_id": "c1",
            "order_status": "delivered",
            "order_purchase_timestamp": "2018-01-15 10:30:00",
        },
    ]).to_csv(
        tmp_path / "olist_orders_dataset.csv",
        index=False,
    )

    pd.DataFrame([
        {
            "order_id": "o1",
            "order_item_id": 1,
            "product_id": "p1",
            "seller_id": "s1",
            "price": 100.0,
            "freight_value": 10.0,
        },
        {
            "order_id": "o1",
            "order_item_id": 2,
            "product_id": "p2",
            "seller_id": "s2",
            "price": 50.0,
            "freight_value": 5.0,
        },
    ]).to_csv(
        tmp_path / "olist_order_items_dataset.csv",
        index=False,
    )

    pd.DataFrame([
        {
            "product_id": "p1",
            "product_category_name": "electronics",
        },
        {
            "product_id": "p2",
            "product_category_name": "books",
        },
    ]).to_csv(
        tmp_path / "olist_products_dataset.csv",
        index=False,
    )

    result = load_order_events(tmp_path)

    assert len(result) == 2
    assert result["event_id"].is_unique
    assert result["item_total"].tolist() == [110.0, 55.0]
    assert set(result["order_id"]) == {"o1"}

