-- Orders per day (canceled orders excluded)
SELECT
  DATE(order_timestamp)    AS order_date,
  COUNT(DISTINCT order_id) AS total_orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_date
ORDER BY order_date
