-- Category-wise Growth Analysis
-- This query analyzes revenue growth by category

WITH category_daily AS (
  SELECT
    event_date,
    category,
    SUM(daily_revenue) AS category_revenue,
    SUM(daily_quantity) AS category_quantity,
    SUM(daily_orders) AS category_orders,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT unique_customers) AS unique_customers
  FROM
    `{project_id}.{dataset_id}.daily_sales`
  WHERE
    event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY
    event_date,
    category
),

category_growth AS (
  SELECT
    event_date,
    category,
    category_revenue,
    category_quantity,
    category_orders,
    unique_products,
    unique_customers,
    -- Day-over-day growth
    LAG(category_revenue) OVER (
      PARTITION BY category
      ORDER BY event_date
    ) AS prev_day_revenue,
    SAFE_DIVIDE(
      category_revenue - LAG(category_revenue) OVER (
        PARTITION BY category
        ORDER BY event_date
      ),
      LAG(category_revenue) OVER (
        PARTITION BY category
        ORDER BY event_date
      )
    ) * 100 AS revenue_growth_pct,
    -- Week-over-week growth
    LAG(category_revenue, 7) OVER (
      PARTITION BY category
      ORDER BY event_date
    ) AS prev_week_revenue,
    SAFE_DIVIDE(
      category_revenue - LAG(category_revenue, 7) OVER (
        PARTITION BY category
        ORDER BY event_date
      ),
      LAG(category_revenue, 7) OVER (
        PARTITION BY category
        ORDER BY event_date
      )
    ) * 100 AS revenue_growth_wow_pct
  FROM
    category_daily
)

SELECT
  event_date,
  category,
  category_revenue,
  category_quantity,
  category_orders,
  unique_products,
  unique_customers,
  revenue_growth_pct,
  revenue_growth_wow_pct,
  -- Category ranking by revenue
  RANK() OVER (
    PARTITION BY event_date
    ORDER BY category_revenue DESC
  ) AS category_rank
FROM
  category_growth
ORDER BY
  event_date DESC,
  category_revenue DESC;

