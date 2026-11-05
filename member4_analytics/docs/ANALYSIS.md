# Analysis findings

Data: sample (synthetic) Olist-style orders, 2018-01-01 to 2018-06-30. Canceled orders are
excluded from all figures. Total sales = price + freight (`item_total`); category and
product revenue use `price` only.

## Headline numbers
- Total sales: **299,674.71**
- Orders: **2,446**
- Average order value: **122.52**
- Canceled orders excluded: 54

## Top categories by revenue
- **toys**: 36,618.77 (14.9% of product revenue)
- **computers_accessories**: 33,538.99 (13.7% of product revenue)
- **furniture_decor**: 28,446.23 (11.6% of product revenue)

## Peak ordering periods
Busiest hours of the day: 16:00 (208 orders), 14:00 (198 orders), 13:00 (176 orders).
Best sales day: 2018-06-03 with 3,180.10.

## Top product
Product `7e876a3d44ed` (auto): 8 units, 4,502.64 revenue.

## Data-quality note
The duplicate-detection integration test caught a double load of the table, caused by the
loader's WRITE_APPEND mode. The table was deleted and reloaded once. Recommendation: make
the loader idempotent.
