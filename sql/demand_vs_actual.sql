-- Demand Prediction vs Actual Sales Comparison
-- This query compares predicted demand with actual sales

WITH predictions AS (
  SELECT
    prediction_date,
    product_id,
    category,
    predicted_demand,
    prediction_horizon,
    model_name,
    model_version
  FROM
    `{project_id}.{dataset_id}.demand_predictions`
  WHERE
    prediction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND prediction_horizon = 'next_day'
),

actual_sales AS (
  SELECT
    event_date,
    product_id,
    category,
    daily_quantity AS actual_quantity,
    daily_revenue AS actual_revenue
  FROM
    `{project_id}.{dataset_id}.daily_sales`
  WHERE
    event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

comparison AS (
  SELECT
    COALESCE(p.prediction_date, a.event_date) AS date,
    COALESCE(p.product_id, a.product_id) AS product_id,
    COALESCE(p.category, a.category) AS category,
    p.predicted_demand,
    a.actual_quantity,
    a.actual_revenue,
    p.model_name,
    p.model_version,
    -- Calculate prediction error
    ABS(p.predicted_demand - COALESCE(a.actual_quantity, 0)) AS absolute_error,
    SAFE_DIVIDE(
      ABS(p.predicted_demand - COALESCE(a.actual_quantity, 0)),
      NULLIF(COALESCE(a.actual_quantity, 0), 0)
    ) * 100 AS percentage_error
  FROM
    predictions p
  FULL OUTER JOIN
    actual_sales a
  ON
    p.prediction_date = a.event_date
    AND p.product_id = a.product_id
)

SELECT
  date,
  product_id,
  category,
  predicted_demand,
  actual_quantity,
  actual_revenue,
  absolute_error,
  percentage_error,
  model_name,
  model_version,
  -- Prediction accuracy indicator
  CASE
    WHEN percentage_error <= 10 THEN 'Excellent'
    WHEN percentage_error <= 20 THEN 'Good'
    WHEN percentage_error <= 30 THEN 'Fair'
    ELSE 'Poor'
  END AS prediction_quality
FROM
  comparison
ORDER BY
  date DESC,
  absolute_error DESC;

