-- Daily sales (canceled orders excluded)
SELECT
  DATE(order_timestamp)        AS order_date,
  ROUND(SUM(price), 2)         AS product_sales,
  ROUND(SUM(freight_value), 2) AS freight_total,
  ROUND(SUM(item_total), 2)    AS total_sales
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_date
ORDER BY order_date
