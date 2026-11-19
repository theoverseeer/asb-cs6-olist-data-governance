-- Olist Operational Dashboard: dataset queries
-- Source: olist_gov.semantic.sales_order_items (one row per order item, BRL)
-- Rules from the view description:
--   * Filter to valid orders (is_valid_order).
--   * Never sum order-level values (review_score, delivery_days, is_late);
--     weight averages of order-level fields by order_weight.

-- ---------------------------------------------------------------
-- op_kpis  (Overview counters + Delivery counters)
-- ---------------------------------------------------------------
SELECT
  COUNT(DISTINCT order_id) AS orders,
  SUM(price) AS sales_brl,
  ROUND((SUM(price) + SUM(freight_value)) / SUM(order_weight), 2) AS aov_brl,
  ROUND(SUM(CASE WHEN has_delivery THEN delivery_days * order_weight END)
    / SUM(CASE WHEN has_delivery THEN order_weight END), 2) AS avg_delivery_days,
  ROUND(100 * SUM(CASE WHEN has_delivery AND is_late THEN order_weight END)
    / SUM(CASE WHEN has_delivery THEN order_weight END), 2) AS late_delivery_pct,
  ROUND(SUM(CASE WHEN has_review THEN review_score * order_weight END)
    / SUM(CASE WHEN has_review THEN order_weight END), 2) AS avg_review_score
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order;

-- ---------------------------------------------------------------
-- op_monthly  (Orders by month, Sales by month)
-- Jan 2017 to Aug 2018 only; other months have too few orders to chart.
-- ---------------------------------------------------------------
SELECT purchase_month AS month, COUNT(DISTINCT order_id) AS orders, SUM(price) AS sales_brl
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order
  AND purchase_month BETWEEN DATE'2017-01-01' AND DATE'2018-08-01'
GROUP BY 1 ORDER BY 1;

-- ---------------------------------------------------------------
-- op_category  (Top 10 categories by sales)
-- ---------------------------------------------------------------
SELECT product_category AS category, SUM(price) AS sales_brl
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- ---------------------------------------------------------------
-- op_late_monthly  (Late deliveries by month, %)
-- ---------------------------------------------------------------
SELECT purchase_month AS month,
       100 * SUM(CASE WHEN is_late THEN order_weight END) / SUM(order_weight) AS late_delivery_pct
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order AND has_delivery
  AND purchase_month BETWEEN DATE'2017-01-01' AND DATE'2018-08-01'
GROUP BY 1 ORDER BY 1;

-- ---------------------------------------------------------------
-- op_state_delivery  (Average delivery days by state)
-- ---------------------------------------------------------------
SELECT customer_state,
       SUM(delivery_days * order_weight) / SUM(order_weight) AS avg_delivery_days
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order AND has_delivery
GROUP BY 1 ORDER BY 2 DESC;

-- ---------------------------------------------------------------
-- op_review_dist  (Review score distribution)
-- ---------------------------------------------------------------
SELECT review_score, ROUND(SUM(order_weight)) AS orders
FROM olist_gov.semantic.sales_order_items
WHERE is_valid_order AND has_review
GROUP BY 1 ORDER BY 1;
