-- Top Selling Products Analysis
-- This query identifies top-selling products with various metrics

WITH product_metrics AS (
  SELECT
    product_id,
    category,
    SUM(daily_revenue) AS total_revenue,
    SUM(daily_quantity) AS total_quantity,
    SUM(daily_orders) AS total_orders,
    AVG(avg_price) AS avg_price,
    COUNT(DISTINCT event_date) AS days_active,
    COUNT(DISTINCT unique_customers) AS unique_customers,
    MAX(event_date) AS last_sale_date
  FROM
    `{project_id}.{dataset_id}.daily_sales`
  WHERE
    event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY
    product_id,
    category
)

SELECT
  product_id,
  category,
  total_revenue,
  total_quantity,
  total_orders,
  avg_price,
  days_active,
  unique_customers,
  last_sale_date,
  -- Revenue per order
  SAFE_DIVIDE(total_revenue, total_orders) AS revenue_per_order,
  -- Average daily quantity
  SAFE_DIVIDE(total_quantity, days_active) AS avg_daily_quantity,
  -- Rankings
  RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
  RANK() OVER (ORDER BY total_quantity DESC) AS quantity_rank,
  RANK() OVER (ORDER BY total_orders DESC) AS orders_rank,
  RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_revenue_rank
FROM
  product_metrics
ORDER BY
  total_revenue DESC
LIMIT
  100;

