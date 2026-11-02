
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "raw"

FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]


def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"Dataset folder not found: {DATA_DIR}"
        )

    for filename in FILES:
        path = DATA_DIR / filename

        print("\n" + "=" * 65)
        print(f"FILE: {filename}")
        print("=" * 65)

        if not path.exists():
            print("Not available in the downloaded dataset.")
            continue

        df = pd.read_csv(path)

        print("Rows:", len(df))
        print("Columns:", len(df.columns))
        print("\nColumn names:")
        print(df.columns.tolist())

        print("\nData types:")
        print(df.dtypes)

        print("\nMissing values:")
        print(df.isna().sum())

        print("\nDuplicate rows:", df.duplicated().sum())

        print("\nFirst 5 rows:")
        print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()