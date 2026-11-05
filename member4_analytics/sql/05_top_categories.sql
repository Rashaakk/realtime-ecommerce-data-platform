-- Top 10 categories by revenue
SELECT
  category,
  ROUND(SUM(price), 2)     AS revenue,
  COUNT(DISTINCT order_id) AS orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY category
ORDER BY revenue DESC
LIMIT 10
