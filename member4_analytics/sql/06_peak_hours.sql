-- Peak ordering hours
SELECT
  EXTRACT(HOUR FROM order_timestamp) AS order_hour,
  COUNT(DISTINCT order_id)           AS orders
FROM {{TABLE}}
WHERE order_status != 'canceled'
GROUP BY order_hour
ORDER BY orders DESC
