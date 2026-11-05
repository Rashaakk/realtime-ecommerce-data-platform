"""Generate sample validated orders (synthetic, Olist-style) as Parquet."""
import uuid
import numpy as np
import pandas as pd
from pathlib import Path

rng = np.random.default_rng(42)
OUT = Path("data/quality/validated")
OUT.mkdir(parents=True, exist_ok=True)

categories = ["health_beauty", "watches_gifts", "bed_bath_table", "sports_leisure",
              "computers_accessories", "furniture_decor", "housewares", "toys",
              "auto", "perfumery"]
statuses = ["delivered", "shipped", "processing", "canceled"]
status_p = [0.90, 0.05, 0.03, 0.02]

# order times: Jan-Jun 2018, busier in the afternoon
day_pool = pd.date_range("2018-01-01", "2018-06-30", freq="D")
hour_p = np.array([1,1,1,1,1,1,2,3,5,6,7,8,8,8,9,9,9,8,7,7,6,5,3,2], dtype=float)
hour_p /= hour_p.sum()

# fixed product catalog so top-products analysis is meaningful
n_products = 300
product_ids = [uuid.uuid4().hex[:12] for _ in range(n_products)]
product_cat = rng.choice(categories, n_products)
product_price = np.round(rng.lognormal(4.0, 0.8, n_products) + 5, 2)

rows = []
for i in range(2500):
    order_id = uuid.uuid4().hex
    customer_id = uuid.uuid4().hex[:16]
    status = rng.choice(statuses, p=status_p)
    day = rng.choice(day_pool)
    hour = rng.choice(24, p=hour_p)
    ts = pd.Timestamp(day) + pd.Timedelta(hours=int(hour),
          minutes=int(rng.integers(0, 60)), seconds=int(rng.integers(0, 60)))
    for item in range(1, int(rng.choice([1, 2, 3], p=[0.8, 0.15, 0.05])) + 1):
        p = int(rng.integers(0, n_products))
        price = float(product_price[p])
        freight = round(float(rng.uniform(5, 30)), 2)
        rows.append({
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "order_item_id": item,
            "customer_id": customer_id,
            "product_id": product_ids[p],
            "seller_id": uuid.uuid4().hex[:12],
            "order_status": status,
            "order_timestamp": ts,
            "category": str(product_cat[p]),
            "price": price,
            "freight_value": freight,
            "item_total": round(price + freight, 2),
        })

df = pd.DataFrame(rows)
df["order_item_id"] = df["order_item_id"].astype("int64")
path = OUT / "sample_orders.parquet"
df.to_parquet(path, index=False, coerce_timestamps="us",
              allow_truncated_timestamps=True)
print(f"Wrote {len(df)} rows ({df.order_id.nunique()} orders) to {path}")