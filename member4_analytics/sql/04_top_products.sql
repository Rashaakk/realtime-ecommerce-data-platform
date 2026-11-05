-- Top 10 products by revenue
SELECT
  product_id,
  category,
  COUNT(*)             AS units_sold,
  ROUND(SUM(price), 2) AS revenue
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY product_id, category
ORDER BY revenue DESC
LIMIT 10
