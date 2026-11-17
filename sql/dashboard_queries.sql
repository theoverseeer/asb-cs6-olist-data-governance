-- Databricks AI/BI dashboard datasets.
-- Replace `olist_gov` with your catalog name (find/replace) before pasting each query into
-- Dashboard -> Data tab -> "Create from SQL". Each block = one dataset. Both dashboards read ONLY from the `semantic` schema.
--
-- DASHBOARD 1 rebuilds the four pages of your earlier Power BI dashboard (same business questions 5.1.x - 5.5.x,
-- same measure definitions as dax/measures.dax). Trend charts use complete months only (2017-01 .. 2018-08),
-- exactly as in the earlier project.
-- Palette from the earlier theme: main #0072B2, problem #D55E00, highlight #E69F00, de-emphasis #BDBDBD (set per widget in AI/BI).

-- =====================================================================================
-- DASHBOARD 1 · OLIST OPERATIONAL PERFORMANCE  (4 pages, same as the Power BI version)
-- =====================================================================================

-- [ds_trust_banner]  ALL PAGES: small counter/text in the header - links this dashboard to the DQ dashboard
SELECT trust_status, pass_rate_pct, item_value_coverage_pct, records_quarantined
FROM olist_gov.semantic.dq_trust_scorecard;

-- ------------------------------- PAGE 1 · EXECUTIVE OVERVIEW (5.1.x)
-- [p1_kpis]  six KPI cards: Sales (Items), Valid Orders, AOV, Unique Customers, On-Time Delivery %, Avg Review Score
SELECT sum(items_value)                                                   AS sales_items_brl,
       count(*)                                                           AS valid_orders,
       sum(order_value) / count(*)                                        AS aov_brl,
       count(DISTINCT customer_hash)                                      AS unique_customers,
       100.0 * (1 - sum(CASE WHEN is_late THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN 1 ELSE 0 END), 0)) AS on_time_delivery_pct,
       avg(review_score)                                                  AS avg_review_score
FROM olist_gov.semantic.orders WHERE is_valid_order;

-- [p1_monthly]  line (sales) + column (valid orders), same x-axis; complete months only
SELECT year_month, sum(items_value) AS sales_items_brl, count(*) AS valid_orders, sum(order_value) / count(*) AS aov_brl
FROM olist_gov.semantic.orders
WHERE is_valid_order AND purchase_month BETWEEN DATE'2017-01-01' AND DATE'2018-08-01'
GROUP BY year_month ORDER BY year_month;

-- [p1_status]  bar: order status mix (delivered in blue, the rest grey)
SELECT order_status, count(*) AS orders, 100.0 * count(*) / sum(count(*)) OVER () AS pct_of_orders
FROM olist_gov.semantic.orders GROUP BY order_status ORDER BY orders DESC;

-- ------------------------------- PAGE 2 · CUSTOMERS (5.2.x)
-- [p2_kpis]  cards: Unique Customers, Repeat Customer Rate, Orders per Customer
SELECT count(DISTINCT customer_hash)                                                           AS unique_customers,
       100.0 * count(DISTINCT CASE WHEN customer_type = 'Repeat' THEN customer_hash END)
             / count(DISTINCT customer_hash)                                                   AS repeat_customer_rate_pct,
       count(*) / count(DISTINCT customer_hash)                                                AS orders_per_customer
FROM olist_gov.semantic.orders WHERE is_valid_order;

-- [p2_states]  map / ranked bar: sales and customers by state (filter: purchase_year)
SELECT customer_state, purchase_year, sum(items_value) AS sales_items_brl, count(*) AS valid_orders,
       count(DISTINCT customer_hash) AS unique_customers
FROM olist_gov.semantic.orders WHERE is_valid_order GROUP BY customer_state, purchase_year;

-- [p2_repeat_vs_onetime]  small multiples: AOV, review, late % by customer type
SELECT customer_type, sum(order_value) / count(*) AS aov_brl, avg(review_score) AS avg_review_score,
       100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN 1 ELSE 0 END), 0) AS late_delivery_pct,
       count(DISTINCT customer_hash) AS customers
FROM olist_gov.semantic.orders WHERE is_valid_order AND customer_type IN ('One-time', 'Repeat') GROUP BY customer_type;

-- ------------------------------- PAGE 3 · PRODUCTS AND SELLERS (5.3.x)
-- [p3_top_categories]  horizontal bar: top 10 categories by item sales (one blue colour)
SELECT product_category, sum(price) AS item_sales_brl, sum(order_weight) AS orders_weighted
FROM olist_gov.semantic.sales_order_items WHERE is_valid_order GROUP BY product_category ORDER BY item_sales_brl DESC LIMIT 10;

-- [p3_aov_by_category]  bar: average order value by category (item grain, weighted so multi-item orders count once; min 500 weighted orders)
SELECT product_category, sum(item_value) / sum(order_weight) AS avg_order_value_in_category_brl, sum(order_weight) AS orders_weighted
FROM olist_gov.semantic.sales_order_items WHERE is_valid_order
GROUP BY product_category HAVING sum(order_weight) >= 500 ORDER BY avg_order_value_in_category_brl DESC;

-- [p3_seller_pareto]  combo: top 50 sellers' sales + cumulative share of ALL sellers' sales
WITH s AS (SELECT seller_id, sum(price) AS item_sales_brl FROM olist_gov.semantic.sales_order_items WHERE is_valid_order GROUP BY seller_id),
r AS (SELECT seller_id, item_sales_brl,
             sum(item_sales_brl) OVER (ORDER BY item_sales_brl DESC, seller_id) / sum(item_sales_brl) OVER () AS cumulative_share,
             row_number() OVER (ORDER BY item_sales_brl DESC, seller_id) AS seller_rank
      FROM s)
SELECT seller_rank, seller_id, item_sales_brl, 100.0 * cumulative_share AS cumulative_sales_pct FROM r WHERE seller_rank <= 50 ORDER BY seller_rank;

-- [p3_top_sellers]  table, conditional formatting on late % (orange above marketplace average)
SELECT seller_id, seller_state, sum(price) AS item_sales_brl,
       100.0 * sum(CASE WHEN is_late THEN order_weight ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN order_weight ELSE 0 END), 0) AS late_pct,
       sum(review_score * order_weight) / nullif(sum(CASE WHEN has_review THEN order_weight ELSE 0 END), 0) AS avg_review
FROM olist_gov.semantic.sales_order_items WHERE is_valid_order
GROUP BY seller_id, seller_state ORDER BY item_sales_brl DESC LIMIT 20;

-- ------------------------------- PAGE 4 · DELIVERY AND CUSTOMER EXPERIENCE (5.4.x, 5.5.x)
-- [p4_kpis]  cards: Avg Delivery Days, Late Delivery %, Avg Days Late, Avg Review Score, Negative Review %
SELECT avg(delivery_days)                                                                                   AS avg_delivery_days,
       100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN 1 ELSE 0 END), 0) AS late_delivery_pct,
       avg(CASE WHEN is_late THEN delay_days END)                                                           AS avg_days_late,
       avg(review_score)                                                                                    AS avg_review_score,
       100.0 * sum(CASE WHEN review_group = 'Negative (1-2)' THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_review THEN 1 ELSE 0 END), 0) AS negative_review_pct
FROM olist_gov.semantic.orders WHERE is_valid_order;

-- [p4_late_by_month]  orange line + average reference line; complete months only
SELECT year_month, 100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN 1 ELSE 0 END), 0) AS late_delivery_pct,
       avg(delivery_days) AS avg_delivery_days
FROM olist_gov.semantic.orders
WHERE has_delivery AND purchase_month BETWEEN DATE'2017-01-01' AND DATE'2018-08-01'
GROUP BY year_month ORDER BY year_month;

-- [p4_late_by_state]  bar: top 10 states by late % (min 300 delivered orders, as in the earlier project)
SELECT customer_state, 100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / count(*) AS late_delivery_pct,
       avg(delivery_days) AS avg_delivery_days, count(*) AS delivered_orders,
       sum(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_orders
FROM olist_gov.semantic.orders WHERE has_delivery
GROUP BY customer_state HAVING count(*) >= 300 ORDER BY late_delivery_pct DESC LIMIT 10;

-- [p4_review_by_delay]  THE key diagnostic (5.5.3): review score by delay bucket; on-time blue, late buckets orange
SELECT delay_bucket, avg(review_score) AS avg_review_score,
       100.0 * sum(CASE WHEN review_group = 'Negative (1-2)' THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_review THEN 1 ELSE 0 END), 0) AS negative_review_pct,
       count(*) AS orders
FROM olist_gov.semantic.orders WHERE has_delivery AND has_review AND is_valid_order GROUP BY delay_bucket ORDER BY delay_bucket;

-- [p4_review_distribution]  column chart, single hue (one latest review per order)
SELECT review_score, count(*) AS reviewed_orders FROM olist_gov.semantic.orders WHERE has_review GROUP BY review_score ORDER BY review_score;

-- [p4_lowest_rated_categories]  bar: bottom 10 categories by average review (min 300 items, item grain weighted)
SELECT product_category, sum(review_score * order_weight) / nullif(sum(CASE WHEN has_review THEN order_weight ELSE 0 END), 0) AS avg_review,
       count(*) AS items
FROM olist_gov.semantic.sales_order_items WHERE is_valid_order
GROUP BY product_category HAVING count(*) >= 300 ORDER BY avg_review ASC LIMIT 10;

-- [p4_insight_late_vs_ontime]  text box (replaces the DAX "Insight: Late vs On-Time Review")
SELECT concat('Late orders score ', format_number(avg(CASE WHEN is_late THEN review_score END), 2), ' vs ',
              format_number(avg(CASE WHEN is_late = false THEN review_score END), 2), ' for on-time orders') AS insight
FROM olist_gov.semantic.orders WHERE has_delivery AND is_valid_order;

-- [extra_payment_mix]  optional: payment type share by value (valid orders)
SELECT payment_type, sum(payment_value) AS payment_value_brl, count(*) AS payments
FROM olist_gov.semantic.payments WHERE is_valid_order GROUP BY payment_type;

-- =====================================================================================
-- DASHBOARD 2 · DATA QUALITY MONITORING  ("Can we trust the data?")
-- =====================================================================================

-- [dq_trust]  Counters: verdict, records evaluated/valid/quarantined, pass rate, failure rate, rule statuses, value coverage
SELECT * FROM olist_gov.semantic.dq_trust_scorecard;

-- [dq_reconciliation]  Table: governed KPIs vs the earlier ungoverned dashboard (REVIEW = gap beyond 0.5%; explain it with quarantine)
SELECT metric, unit, baseline_value, governed_value, difference, difference_pct, status FROM olist_gov.semantic.dq_kpi_reconciliation ORDER BY metric;

-- [dq_by_table]  Stacked bar: valid vs quarantined vs flagged per source table
SELECT table_name, valid_count - valid_with_warnings_count AS valid_clean, valid_with_warnings_count AS valid_with_warnings,
       quarantined_count, evaluated_count, pass_rate_pct, failure_rate_pct
FROM olist_gov.semantic.dq_table_summary ORDER BY quarantined_count DESC;

-- [dq_by_dimension]  Bar: failures and pass rate per DQ dimension
SELECT dimension, rules, failed_checks, pass_rate_pct, rules_failed FROM olist_gov.semantic.dq_dimension_summary ORDER BY failed_checks DESC;

-- [dq_by_rule]  Table with conditional formatting on rule_status (the scorecard, like slide 31 of the lecture)
SELECT rule_id, rule_description, dimension, table_name, failure_action, failed_count, evaluated_count, pass_rate_pct,
       target_pass_rate_pct, rule_status, steward_role
FROM olist_gov.semantic.dq_rule_scorecard ORDER BY failed_count DESC;

-- [dq_top_failing]  Bar: most frequently failing rules
SELECT rule_id, table_name, dimension, failed_count FROM olist_gov.semantic.dq_rule_scorecard
WHERE failed_count > 0 ORDER BY failed_count DESC LIMIT 10;

-- [dq_rule_status_mix]  Donut: PASSED / WARNING / FAILED rules
SELECT rule_status, count(*) AS rules FROM olist_gov.semantic.dq_rule_scorecard GROUP BY rule_status;

-- [dq_by_steward]  Table: workload per steward
SELECT steward_role, data_domain, sum(failed_count) AS failed_checks, sum(CASE WHEN rule_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_rules
FROM olist_gov.semantic.dq_rule_scorecard GROUP BY steward_role, data_domain ORDER BY failed_checks DESC;

-- [dq_value_at_risk]  Bar: BRL value of quarantined items by rule
SELECT * FROM olist_gov.semantic.dq_quarantined_item_value ORDER BY item_price_brl DESC;

-- [dq_trend]  Line: pass rate over runs
SELECT run_ts, pass_rate_pct, quarantined FROM olist_gov.semantic.dq_run_trend ORDER BY run_ts;

-- [dq_investigate]  Drill-down table (filters: source_table, rule_id): why a record failed + the original row
SELECT source_table, record_key, rule_id, rule_description, failed_rule_count, source_file, record_payload
FROM olist_gov.semantic.dq_quarantine_detail ORDER BY failed_rule_count DESC, source_table LIMIT 500;

-- [dq_injection_evidence]  Table: proof that each rule catches what it should (from notebook 10)
SELECT rule_id, table_name, defect, injected_rows, rule_failed_count, result
FROM olist_gov.governance.dq_injection_test_results
WHERE test_run_id = (SELECT max(test_run_id) FROM olist_gov.governance.dq_injection_test_results) ORDER BY rule_id;
