-- Daily Revenue Analytics
-- This query calculates daily revenue metrics and trends

WITH daily_metrics AS (
  SELECT
    event_date,
    SUM(daily_revenue) AS total_revenue,
    SUM(daily_quantity) AS total_quantity,
    SUM(daily_orders) AS total_orders,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT unique_customers) AS unique_customers,
    AVG(avg_price) AS avg_price_across_products
  FROM
    `{project_id}.{dataset_id}.daily_sales`
  WHERE
    event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY
    event_date
)

SELECT
  event_date,
  total_revenue,
  total_quantity,
  total_orders,
  unique_products,
  unique_customers,
  avg_price_across_products,
  -- Day-over-day growth
  LAG(total_revenue) OVER (ORDER BY event_date) AS prev_day_revenue,
  SAFE_DIVIDE(
    total_revenue - LAG(total_revenue) OVER (ORDER BY event_date),
    LAG(total_revenue) OVER (ORDER BY event_date)
  ) * 100 AS revenue_growth_pct,
  -- 7-day moving average
  AVG(total_revenue) OVER (
    ORDER BY event_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS revenue_7d_ma,
  -- 30-day moving average
  AVG(total_revenue) OVER (
    ORDER BY event_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS revenue_30d_ma
FROM
  daily_metrics
ORDER BY
  event_date DESC;

