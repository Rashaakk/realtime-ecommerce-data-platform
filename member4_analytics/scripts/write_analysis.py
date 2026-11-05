import os
from pathlib import Path

from google.cloud import bigquery

PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")
if not PROJECT:
    raise SystemExit("Set BIGQUERY_PROJECT_ID first.")

client = bigquery.Client(project=PROJECT)
T = f"`{PROJECT}.{DATASET}.{TABLE}`"
OK = "order_status != 'canceled'"


def q(sql):
    return list(client.query(sql).result())


tot = q(f"""SELECT ROUND(SUM(item_total),2) s, COUNT(DISTINCT order_id) o,
            ROUND(SUM(item_total)/COUNT(DISTINCT order_id),2) a,
            MIN(DATE(order_timestamp)) d1, MAX(DATE(order_timestamp)) d2
            FROM {T} WHERE {OK}""")[0]
cats = q(f"""SELECT category, ROUND(SUM(price),2) revenue,
             ROUND(100*SUM(price)/SUM(SUM(price)) OVER(),1) pct
             FROM {T} WHERE {OK} GROUP BY category ORDER BY revenue DESC LIMIT 3""")
hours = q(f"""SELECT EXTRACT(HOUR FROM order_timestamp) h, COUNT(DISTINCT order_id) n
              FROM {T} WHERE {OK} GROUP BY h ORDER BY n DESC LIMIT 3""")
prod = q(f"""SELECT product_id, category, COUNT(*) units, ROUND(SUM(price),2) revenue
             FROM {T} WHERE {OK} GROUP BY 1,2 ORDER BY revenue DESC LIMIT 1""")[0]
day = q(f"""SELECT DATE(order_timestamp) d, ROUND(SUM(item_total),2) s
            FROM {T} WHERE {OK} GROUP BY d ORDER BY s DESC LIMIT 1""")[0]
canc = q(f"""SELECT COUNT(DISTINCT order_id) n FROM {T} WHERE NOT ({OK})""")[0]

cat_lines = "\n".join(
    f"- **{r.category}**: {r.revenue:,.2f} ({r.pct}% of product revenue)" for r in cats)
hour_lines = ", ".join(f"{int(r.h):02d}:00 ({r.n} orders)" for r in hours)

text = f"""# Analysis findings

Data: sample (synthetic) Olist-style orders, {tot.d1} to {tot.d2}. Canceled orders are
excluded from all figures. Total sales = price + freight (`item_total`); category and
product revenue use `price` only.

## Headline numbers
- Total sales: **{tot.s:,.2f}**
- Orders: **{tot.o:,}**
- Average order value: **{tot.a:,.2f}**
- Canceled orders excluded: {canc.n:,}

## Top categories by revenue
{cat_lines}

## Peak ordering periods
Busiest hours of the day: {hour_lines}.
Best sales day: {day.d} with {day.s:,.2f}.

## Top product
Product `{prod.product_id}` ({prod.category}): {prod.units} units, {prod.revenue:,.2f} revenue.

## Data-quality note
The duplicate-detection integration test caught a double load of the table, caused by the
loader's WRITE_APPEND mode. The table was deleted and reloaded once. Recommendation: make
the loader idempotent.
"""
out = Path("member4_analytics/docs")
out.mkdir(parents=True, exist_ok=True)
(out / "ANALYSIS.md").write_text(text, encoding="utf-8")
print("Wrote ANALYSIS.md")