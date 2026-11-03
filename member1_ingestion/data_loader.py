
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "raw"
OUTPUT_DIR = ROOT / "data" / "processed"


def load_order_events(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    orders_path = data_dir / "olist_orders_dataset.csv"
    items_path = data_dir / "olist_order_items_dataset.csv"
    products_path = data_dir / "olist_products_dataset.csv"

    for path in (orders_path, items_path, products_path):
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")

    orders = pd.read_csv(orders_path)
    items = pd.read_csv(items_path)
    products = pd.read_csv(products_path)

    # Each order can have multiple item rows.
    df = items.merge(
        orders[
            [
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
            ]
        ],
        on="order_id",
        how="inner",
        validate="many_to_one",
    )

    # Attach product category.
    df = df.merge(
        products[["product_id", "product_category_name"]],
        on="product_id",
        how="left",
        validate="many_to_one",
    )

    # Remove rows missing essential identifiers.
    df = df.dropna(
        subset=[
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_purchase_timestamp",
            "price",
            "freight_value",
        ]
    ).copy()

    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"],
        errors="coerce",
    )

    df = df.dropna(subset=["order_purchase_timestamp"]).copy()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(
        df["freight_value"], errors="coerce"
    )

    df = df.dropna(subset=["price", "freight_value"]).copy()

    # Reject invalid monetary values.
    df = df[
        (df["price"] >= 0)
        & (df["freight_value"] >= 0)
    ].copy()

    # Stable event ID for identifying repeated order-item events.
    df["event_id"] = (
        df["order_id"].astype(str)
        + "_"
        + df["order_item_id"].astype(int).astype(str)
    )

    events = pd.DataFrame({
        "event_id": df["event_id"],
        "order_id": df["order_id"].astype(str),
        "order_item_id": df["order_item_id"].astype(int),
        "customer_id": df["customer_id"].astype(str),
        "product_id": df["product_id"].astype(str),
        "seller_id": df["seller_id"].astype(str),
        "order_status": df["order_status"].fillna("unknown").astype(str),
        "order_timestamp": (
            df["order_purchase_timestamp"].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        ),
        "category": df["product_category_name"].fillna("unknown").astype(str),
        "price": df["price"].astype(float),
        "freight_value": df["freight_value"].astype(float),
    })

    events["item_total"] = (
        events["price"] + events["freight_value"]
    ).round(2)

    events = events.drop_duplicates(subset=["event_id"])

    return events.reset_index(drop=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    events = load_order_events()

    output_path = OUTPUT_DIR / "order_events.jsonl"

    events.to_json(
        output_path,
        orient="records",
        lines=True,
        force_ascii=False,
    )

    print("Data loading completed.")
    print("Events generated:", len(events))
    print("Unique event IDs:", events["event_id"].nunique())
    print("Output file:", output_path)
    print("\nSample records:")
    print(events.head().to_string(index=False))


if __name__ == "__main__":
    main()