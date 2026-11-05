import os

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery

PROJECT = os.getenv("BIGQUERY_PROJECT_ID")
DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce")
TABLE = os.getenv("BIGQUERY_TABLE", "validated_orders")

st.set_page_config(page_title="E-commerce Dashboard", layout="wide")
st.title("Real-time E-commerce Analytics")

if not PROJECT:
    st.error("Set the BIGQUERY_PROJECT_ID environment variable before starting the dashboard.")
    st.stop()

T = f"`{PROJECT}.{DATASET}.{TABLE}`"
client = bigquery.Client(project=PROJECT)


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()


exclude_canceled = st.sidebar.checkbox("Exclude canceled orders", value=True)
WHERE = "WHERE order_status != 'canceled'" if exclude_canceled else ""

# ---- KPIs ----
kpi = q(f"""
    SELECT ROUND(SUM(item_total), 2) AS sales,
           COUNT(DISTINCT order_id) AS orders
    FROM {T} {WHERE}
""")
sales = float(kpi.sales[0] or 0)
orders = int(kpi.orders[0] or 0)
c1, c2, c3 = st.columns(3)
c1.metric("Total Sales", f"{sales:,.2f}")
c2.metric("Total Orders", f"{orders:,}")
c3.metric("Avg Order Value", f"{(sales / orders if orders else 0):,.2f}")

# ---- Daily sales ----
daily = q(f"""
    SELECT DATE(order_timestamp) AS order_date,
           ROUND(SUM(item_total), 2) AS total_sales
    FROM {T} {WHERE}
    GROUP BY order_date ORDER BY order_date
""")
st.plotly_chart(
    px.line(daily, x="order_date", y="total_sales", title="Daily Sales"),
    use_container_width=True,
)

left, right = st.columns(2)

# ---- Top categories ----
cats = q(f"""
    SELECT category, ROUND(SUM(price), 2) AS revenue
    FROM {T} {WHERE}
    GROUP BY category ORDER BY revenue DESC LIMIT 10
""")
left.plotly_chart(
    px.bar(cats, x="category", y="revenue", title="Top 10 Categories by Revenue"),
    use_container_width=True,
)

# ---- Orders by hour ----
hours = q(f"""
    SELECT EXTRACT(HOUR FROM order_timestamp) AS order_hour,
           COUNT(DISTINCT order_id) AS orders
    FROM {T} {WHERE}
    GROUP BY order_hour ORDER BY order_hour
""")
right.plotly_chart(
    px.bar(hours, x="order_hour", y="orders", title="Orders by Hour of Day"),
    use_container_width=True,
)

# ---- Top products ----
st.subheader("Top 10 Products")
top_products = q(f"""
    SELECT product_id, category, COUNT(*) AS units_sold,
           ROUND(SUM(price), 2) AS revenue
    FROM {T} {WHERE}
    GROUP BY product_id, category ORDER BY revenue DESC LIMIT 10
""")
st.dataframe(top_products, use_container_width=True)