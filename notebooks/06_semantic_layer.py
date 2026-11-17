# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Semantic layer
# MAGIC Views that hide joins, surrogate keys and technical flags behind business-friendly names. **Both dashboards read only from `semantic`.**
# MAGIC
# MAGIC *Operational:* `orders`, `sales_order_items`, `customers`, `payments`, `reviews`  ·  *Data quality:* `dq_*` views.
# MAGIC
# MAGIC Metric definitions are fixed here once (and in the business glossary), and **match the earlier Case Study 4 Power BI dashboard** so the two can be reconciled:
# MAGIC * **Valid order** (business term) = status not `canceled`/`unavailable` → `is_valid_order`. Not to be confused with a *valid record* (passed data quality rules).
# MAGIC * **Sales (Items)** = `SUM(items_value)` on valid orders (freight excluded) · **Order value** = items + freight · **AOV** = order value / valid orders.
# MAGIC * **Unique customers** = distinct `customer_hash` (person, pseudonymised `customer_unique_id`) with a valid order.
# MAGIC * **Late delivery %** = orders delivered after the estimated *date* / delivered orders that have a delivered date.
# MAGIC * **Avg review score** = mean of the latest review per order.
# MAGIC
# MAGIC Money columns are in BRL; the view comments say so.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

A = lambda t: T("analytics", t)
SEM = lambda t: T("semantic", t)
G = lambda t: T("governance", t)

# COMMAND ----------

# MAGIC %md ## Operational views

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('orders')}
COMMENT 'One row per valid-record order (BRL). Order-grain facts: order value, delivery, review, payment. Use for counts, AOV, customers, delivery KPIs.' AS
SELECT o.order_id, o.order_status, o.is_valid_order, o.is_delivered, o.has_delivery,
       o.purchase_ts AS purchase_timestamp, o.purchase_date, o.purchase_month, d.year_month, o.order_year AS purchase_year,
       c.customer_unique_hash AS customer_hash, c.customer_type, o.customer_state, o.customer_city,
       o.items_count, o.sellers_count, o.items_value, o.freight_value, o.order_value, o.payment_value, o.payment_type_primary, o.installments_max,
       o.delivered_date, o.estimated_date, o.delivery_days, o.estimated_days, o.delay_days, o.is_late, o.delay_bucket,
       o.review_score, o.has_review, o.review_group, o.approval_hours, o.is_complete_order, o.has_dq_warning
FROM {A('fact_orders')} o
JOIN {A('dim_date')} d ON o.purchase_date_key = d.date_key
JOIN {A('dim_customer')} c ON o.customer_key = c.customer_key""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('sales_order_items')}
COMMENT 'One row per order item (BRL). For sales by category/seller. Never sum order-level values here; use order_weight for averages of order-level fields.' AS
SELECT f.order_id, f.order_item_id, f.order_status, f.is_valid_order, o.purchase_date, o.purchase_month, o.year_month, o.purchase_year,
       o.customer_state, s.seller_id, s.seller_city, s.seller_state, p.product_id, p.category_en AS product_category,
       f.price, f.freight_value, f.item_value, f.order_weight,
       o.review_score, o.has_review, o.has_delivery, o.is_late, o.delay_days, o.delivery_days, o.is_complete_order
FROM {A('fact_order_items')} f
JOIN {SEM('orders')} o ON f.order_id = o.order_id
JOIN {A('dim_seller')} s ON f.seller_key = s.seller_key
JOIN {A('dim_product')} p ON f.product_key = p.product_key""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('customers')}
COMMENT 'One row per person (pseudonymised customer_unique_id): order counts and one-time/repeat type.' AS
SELECT customer_unique_hash AS customer_hash, customer_state, customer_city, orders_total, valid_orders,
       first_purchase_ts, last_purchase_ts, customer_type
FROM {A('dim_customer')}""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('payments')}
COMMENT 'One row per payment (BRL).' AS
SELECT p.order_id, p.payment_sequential, p.payment_type, p.payment_installments AS installments, p.payment_value,
       d.month_start AS purchase_month, o.customer_state, o.is_valid_order
FROM {A('fact_payments')} p
JOIN {A('dim_date')} d ON p.purchase_date_key = d.date_key
JOIN {A('fact_orders')} o ON p.order_id = o.order_id""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('reviews')}
COMMENT 'One row per review. is_latest_review = the single review per order used for scores. Free-text comments excluded (may contain personal data).' AS
SELECT r.review_id, r.order_id, r.review_score, r.is_latest_review, d.date AS review_date, d.month_start AS review_month,
       r.has_comment, r.response_hours, o.order_status, o.customer_state, o.delivery_days, o.is_late, o.delay_bucket
FROM {A('fact_reviews')} r
JOIN {A('dim_date')} d ON r.review_date_key = d.date_key
JOIN {SEM('orders')} o ON r.order_id = o.order_id""")

# COMMAND ----------

# MAGIC %md ## Data quality views (latest run unless noted)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_table_summary')}
COMMENT 'Latest run: records evaluated / valid / quarantined / flagged per source table.' AS
SELECT run_id, run_ts, table_name, evaluated_count, valid_count, quarantined_count, valid_with_warnings_count,
       round(100.0 * valid_count / nullif(evaluated_count,0), 3) AS pass_rate_pct,
       round(100.0 * quarantined_count / nullif(evaluated_count,0), 3) AS failure_rate_pct
FROM {G('dq_run_summary')}
WHERE run_id = (SELECT max(run_id) FROM {G('dq_run_summary')})""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_rule_scorecard')}
COMMENT 'Latest run: one row per rule with pass rate vs target and PASSED / WARNING / FAILED status.' AS
SELECT *, CASE WHEN pass_rate_pct >= target_pass_rate_pct THEN 'PASSED'
               WHEN failure_action = 'FLAG' THEN 'WARNING' ELSE 'FAILED' END AS rule_status
FROM (
  SELECT r.run_id, r.run_ts, r.rule_id, g.rule_description, r.dimension, r.table_name, g.field, r.failure_action,
         g.data_domain, g.owner_role, g.steward_role, r.evaluated_count, r.failed_count,
         r.evaluated_count - r.failed_count AS passed_count,
         round(100.0 * (r.evaluated_count - r.failed_count) / nullif(r.evaluated_count,0), 3) AS pass_rate_pct,
         g.target_pass_rate AS target_pass_rate_pct
  FROM {G('dq_rule_results')} r JOIN {G('dq_rule_registry')} g ON r.rule_id = g.rule_id
  WHERE r.run_id = (SELECT max(run_id) FROM {G('dq_rule_results')}))""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_dimension_summary')}
COMMENT 'Latest run: rule failures and pass rate per data quality dimension.' AS
SELECT dimension, count(*) AS rules, sum(failed_count) AS failed_checks, sum(evaluated_count) AS evaluated_checks,
       round(100.0 * (1 - sum(failed_count) / nullif(sum(evaluated_count),0)), 3) AS pass_rate_pct,
       sum(CASE WHEN rule_status = 'FAILED' THEN 1 ELSE 0 END) AS rules_failed
FROM {SEM('dq_rule_scorecard')} GROUP BY dimension""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_rule_trend')}
COMMENT 'All runs: pass rate per rule over time.' AS
SELECT run_id, run_ts, rule_id, table_name, dimension, failure_action, evaluated_count, failed_count,
       round(100.0 * (evaluated_count - failed_count) / nullif(evaluated_count,0), 3) AS pass_rate_pct
FROM {G('dq_rule_results')}""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_run_trend')}
COMMENT 'All runs: overall record-level pass rate.' AS
SELECT run_id, min(run_ts) AS run_ts, sum(evaluated_count) AS evaluated, sum(valid_count) AS valid, sum(quarantined_count) AS quarantined,
       round(100.0 * sum(valid_count) / nullif(sum(evaluated_count),0), 3) AS pass_rate_pct
FROM {G('dq_run_summary')} GROUP BY run_id""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_quarantine_detail')}
COMMENT 'One row per (quarantined record, failed rule). Use for steward investigation; record_payload holds the original row as received.' AS
SELECT q.run_id, q.source_table, q.record_key, q.failed_rule AS rule_id, g.dimension, g.rule_description, g.field,
       g.steward_role, q.failed_rule_count, q.source_file, q.quarantined_at, q.record_payload
FROM (SELECT *, explode(failed_rules) AS failed_rule FROM {T('quarantine','records')}) q
JOIN {G('dq_rule_registry')} g ON g.rule_id = q.failed_rule""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_trust_scorecard')}
COMMENT 'Single-row answer to: can we trust the data behind the operational dashboard?' AS
WITH t AS (SELECT sum(evaluated_count) AS evaluated, sum(valid_count) AS valid, sum(quarantined_count) AS quarantined
           FROM {SEM('dq_table_summary')}),
raw_val AS (SELECT sum(try_cast(price AS DECIMAL(14,2)) + try_cast(freight_value AS DECIMAL(14,2))) AS v FROM {T('raw','order_items')}),
fact_val AS (SELECT sum(item_value) AS v FROM {A('fact_order_items')}),
rules AS (SELECT count(*) AS total_rules,
                 sum(CASE WHEN rule_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_rules,
                 sum(CASE WHEN rule_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_rules,
                 sum(CASE WHEN rule_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_rules FROM {SEM('dq_rule_scorecard')})
SELECT t.evaluated AS records_evaluated, t.valid AS records_valid, t.quarantined AS records_quarantined,
       round(100.0 * t.valid / nullif(t.evaluated,0), 3) AS pass_rate_pct,
       round(100.0 * t.quarantined / nullif(t.evaluated,0), 3) AS failure_rate_pct,
       rules.total_rules, rules.passed_rules, rules.warning_rules, rules.failed_rules,
       raw_val.v AS raw_item_value_brl, fact_val.v AS analytics_item_value_brl,
       round(100.0 * fact_val.v / nullif(raw_val.v,0), 3) AS item_value_coverage_pct,
       CASE WHEN rules.failed_rules = 0 AND 100.0 * fact_val.v / nullif(raw_val.v,0) >= 99 THEN 'TRUSTED'
            WHEN 100.0 * fact_val.v / nullif(raw_val.v,0) >= 95 THEN 'USE WITH CAUTION'
            ELSE 'NOT TRUSTED' END AS trust_status
FROM t CROSS JOIN rules CROSS JOIN raw_val CROSS JOIN fact_val""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_quarantined_item_value')}
COMMENT 'Latest run: BRL value of order items that were quarantined, by failed rule (revenue at risk).' AS
SELECT rule_id, rule_description, count(DISTINCT record_key) AS items,
       sum(try_cast(get_json_object(record_payload, '$.price') AS DECIMAL(14,2))) AS item_price_brl
FROM {SEM('dq_quarantine_detail')} WHERE source_table = 'order_items' GROUP BY rule_id, rule_description""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation with the earlier (ungoverned) Case Study 4 dashboard
# MAGIC The baseline figures below are the ones **published in your earlier project** (`asb-cs4-olist-dashboard` README / `validation_output.txt`), stored as reference data.
# MAGIC The view compares them with the governed model. A difference is not automatically a bug: it should be explained by quarantined records
# MAGIC (see `semantic.dq_quarantine_detail`). A `REVIEW` status means the gap is larger than the tolerance and a steward should look at it.

# COMMAND ----------

BASELINE = [
 ("Orders (all)", 99441.0, "count"), ("Order items", 112650.0, "count"),
 ("Sum of item prices, all orders (BRL)", 13591643.70, "brl"), ("Sum of payments (BRL)", 16008872.12, "brl"),
 ("Valid orders", 98207.0, "count"), ("Unique customers (valid orders)", 94990.0, "count"),
 ("Sales (Items), valid orders (BRL)", 13494400.74, "brl"), ("AOV (BRL)", 160.23, "brl"),
 ("Delivered orders with delivered date", 96470.0, "count"), ("Late delivery %", 6.77, "pct"),
 ("Avg delivery days", 12.5, "days"), ("Reviewed orders", 98673.0, "count"), ("Avg review score", 4.09, "score"),
]
spark.createDataFrame([(m, v, u, "asb-cs4-olist-dashboard README / docs/validation_output.txt") for m, v, u in BASELINE],
                      "metric string, baseline_value double, unit string, source string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(G("baseline_kpis"))

spark.sql(f"""
CREATE OR REPLACE VIEW {SEM('dq_kpi_reconciliation')}
COMMENT 'Governed KPIs vs the figures published by the earlier ungoverned dashboard; differences should be explained by quarantined records.' AS
WITH f AS (SELECT * FROM {A('fact_orders')}),
g AS (
  SELECT count(*) AS orders_all,
         (SELECT count(*) FROM {A('fact_order_items')}) AS items_all,
         (SELECT sum(price) FROM {A('fact_order_items')}) AS price_all,
         (SELECT sum(payment_value) FROM {A('fact_payments')}) AS pay_all,
         sum(CASE WHEN is_valid_order THEN 1 ELSE 0 END) AS valid_orders,
         count(DISTINCT CASE WHEN is_valid_order THEN customer_key END) AS unique_customers,
         sum(CASE WHEN is_valid_order THEN items_value ELSE 0 END) AS sales_items,
         sum(CASE WHEN is_valid_order THEN order_value ELSE 0 END) / nullif(sum(CASE WHEN is_valid_order THEN 1 ELSE 0 END),0) AS aov,
         sum(CASE WHEN has_delivery THEN 1 ELSE 0 END) AS delivered_orders,
         100.0 * sum(CASE WHEN is_late THEN 1 ELSE 0 END) / nullif(sum(CASE WHEN has_delivery THEN 1 ELSE 0 END),0) AS late_pct,
         avg(delivery_days) AS avg_delivery_days,
         sum(CASE WHEN has_review THEN 1 ELSE 0 END) AS reviewed_orders,
         avg(review_score) AS avg_review FROM f),
m AS (
  SELECT stack(13,
    'Orders (all)', CAST(orders_all AS DOUBLE), 'Order items', CAST(items_all AS DOUBLE),
    'Sum of item prices, all orders (BRL)', CAST(price_all AS DOUBLE), 'Sum of payments (BRL)', CAST(pay_all AS DOUBLE),
    'Valid orders', CAST(valid_orders AS DOUBLE), 'Unique customers (valid orders)', CAST(unique_customers AS DOUBLE),
    'Sales (Items), valid orders (BRL)', CAST(sales_items AS DOUBLE), 'AOV (BRL)', CAST(aov AS DOUBLE),
    'Delivered orders with delivered date', CAST(delivered_orders AS DOUBLE), 'Late delivery %', CAST(late_pct AS DOUBLE),
    'Avg delivery days', CAST(avg_delivery_days AS DOUBLE), 'Reviewed orders', CAST(reviewed_orders AS DOUBLE),
    'Avg review score', CAST(avg_review AS DOUBLE)) AS (metric, governed_value)
  FROM g)
SELECT b.metric, b.unit, b.baseline_value, round(m.governed_value, 4) AS governed_value,
       round(m.governed_value - b.baseline_value, 4) AS difference,
       round(100.0 * (m.governed_value - b.baseline_value) / nullif(b.baseline_value,0), 4) AS difference_pct,
       CASE WHEN abs(100.0 * (m.governed_value - b.baseline_value) / nullif(b.baseline_value,0)) <= 0.5 THEN 'WITHIN TOLERANCE' ELSE 'REVIEW' END AS status
FROM {G('baseline_kpis')} b JOIN m ON m.metric = b.metric""")

print("semantic layer ready:", [r.tableName for r in spark.sql(f"SHOW VIEWS IN {CATALOG}.semantic").collect()])
