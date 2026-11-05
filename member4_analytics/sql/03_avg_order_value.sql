-- Average order value (canceled orders excluded)
SELECT
  ROUND(SUM(item_total) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM {{TABLE}}
WHERE order_status != 'canceled'
