# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Dimensional model (facts + dimensions) - built from `valid` only
# MAGIC
# MAGIC | Table | Grain (one row per …) |
# MAGIC |---|---|
# MAGIC | `fact_orders` | **order** (`order_id`) - order counts, order value, delivery, review, payment roll-ups |
# MAGIC | `fact_order_items` | **order item** (`order_id`, `order_item_id`) - sales by category / seller; carries `order_weight` |
# MAGIC | `fact_payments` | **payment** (`order_id`, `payment_sequential`) |
# MAGIC | `fact_reviews` | **review on an order** (`review_id`, `order_id`); `is_latest_review` marks the one review per order used for scores |
# MAGIC | `dim_date` | calendar day |
# MAGIC | `dim_customer` | **person** (`customer_unique_id`, stored only as a salted hash). `customer_id` is order-scoped, so it is not the customer |
# MAGIC | `dim_product`, `dim_seller` | product / seller |
# MAGIC | `dim_geography` | 5-digit zip prefix (averaged coordinates) |
# MAGIC
# MAGIC **Definitions carried over from the earlier Case Study 4 dashboard** (so both projects report the same business numbers):
# MAGIC valid order = status not `canceled`/`unavailable` · Sales = item price (no freight) on valid orders · order value = price + freight ·
# MAGIC delivery metrics only for `delivered` orders that have a delivered date, compared by **calendar date** · one (latest) review per order ·
# MAGIC repeat customer = 2+ valid orders by the same `customer_unique_id` · `order_weight = 1 / items in order` for order-level averages on the item grain.
# MAGIC
# MAGIC Surrogate keys are `xxhash64(business key)` - deterministic, so facts compute them without a join; the integrity rules guarantee the dimension rows exist.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

SALT = get_salt()
V = lambda t: T("valid", t)
A = lambda t: T("analytics", t)

def ctas(name, sql, comment):
    spark.sql(f"CREATE OR REPLACE TABLE {A(name)} COMMENT '{comment}' AS {sql}")
    print(f"{name:18s} {spark.table(A(name)).count():>10,} rows")

# COMMAND ----------

# ---------------------------------------------------------------- dimensions
ctas("dim_date", f"""
WITH bounds AS (
  SELECT to_date(min(order_purchase_timestamp)) AS mn,
         greatest(to_date(max(order_estimated_delivery_date)), to_date(max(order_delivered_customer_date)),
                  (SELECT to_date(max(review_answer_timestamp)) FROM {V('reviews')})) AS mx
  FROM {V('orders')})
SELECT CAST(date_format(d,'yyyyMMdd') AS INT) AS date_key, d AS date, year(d) AS year, quarter(d) AS quarter,
       month(d) AS month, date_format(d,'MMM') AS month_name, date_format(d,'yyyy-MM') AS year_month, trunc(d,'MM') AS month_start,
       weekofyear(d) AS week_of_year, dayofweek(d) AS day_of_week, date_format(d,'EEEE') AS day_name,
       dayofweek(d) IN (1,7) AS is_weekend
FROM (SELECT explode(sequence(mn, mx, interval 1 day)) AS d FROM bounds)""",
     "Calendar dimension. Grain: one row per day.")

ctas("dim_customer", f"""
WITH co AS (
  SELECT c.customer_unique_id, o.order_id, o.order_purchase_timestamp AS pts, o.order_status, c.customer_state, c.customer_city
  FROM {V('orders')} o JOIN {V('customers')} c ON c.customer_id = o.customer_id),
latest AS (
  SELECT customer_unique_id, customer_state, customer_city,
         row_number() OVER (PARTITION BY customer_unique_id ORDER BY pts DESC, order_id) AS rn FROM co),
cnt AS (
  SELECT customer_unique_id, count(*) AS orders_total,
         sum(CASE WHEN order_status NOT IN ('canceled','unavailable') THEN 1 ELSE 0 END) AS valid_orders,
         min(pts) AS first_purchase_ts, max(pts) AS last_purchase_ts
  FROM co GROUP BY customer_unique_id)
SELECT xxhash64(cnt.customer_unique_id) AS customer_key,
       sha2(concat('{SALT}', cnt.customer_unique_id), 256) AS customer_unique_hash,
       l.customer_state, l.customer_city, cnt.orders_total, cnt.valid_orders, cnt.first_purchase_ts, cnt.last_purchase_ts,
       CASE WHEN cnt.valid_orders >= 2 THEN 'Repeat' WHEN cnt.valid_orders = 1 THEN 'One-time' ELSE 'No valid order' END AS customer_type
FROM cnt JOIN latest l ON l.customer_unique_id = cnt.customer_unique_id AND l.rn = 1""",
     "Customer dimension. Grain: one row per person (customer_unique_id, pseudonymized with a salted SHA-256). State/city = latest order.")

ctas("dim_seller", f"""
SELECT xxhash64(seller_id) AS seller_key, seller_id, seller_city, seller_state, seller_zip_code_prefix
FROM {V('sellers')}""", "Seller dimension. Grain: one row per seller_id.")

ctas("dim_product", f"""
SELECT xxhash64(p.product_id) AS product_key, p.product_id,
       p.product_category_name AS category_pt,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown') AS category_en,
       t.product_category_name_english IS NOT NULL AS category_is_translated,
       p.product_photos_qty, p.product_weight_g,
       p.product_length_cm * p.product_height_cm * p.product_width_cm AS volume_cm3,
       p.dq_warning_count > 0 AS has_dq_warning
FROM {V('products')} p
LEFT JOIN {V('category_translation')} t ON p.product_category_name = t.product_category_name""",
     "Product dimension. Grain: one row per product_id. Untranslated categories keep the Portuguese label; missing = unknown.")

ctas("dim_geography", f"""
SELECT xxhash64(geolocation_zip_code_prefix) AS geography_key, geolocation_zip_code_prefix AS zip_code_prefix,
       mode(geolocation_city) AS city, mode(geolocation_state) AS state,
       avg(geolocation_lat) AS latitude, avg(geolocation_lng) AS longitude, count(*) AS source_point_count
FROM {V('geolocation')} GROUP BY geolocation_zip_code_prefix""",
     "Geography dimension. Grain: one row per 5-digit zip prefix; coordinates are the average of valid source points.")

# COMMAND ----------

# ---------------------------------------------------------------- facts
ctas("fact_orders", f"""
WITH cust AS (SELECT customer_id, customer_unique_id, customer_state, customer_city FROM {V('customers')}),
items AS (
  SELECT order_id, count(*) AS items_count, count(DISTINCT seller_id) AS sellers_count,
         sum(price) AS items_value, sum(freight_value) AS freight_value
  FROM {V('order_items')} GROUP BY order_id),
raw_items AS (
  SELECT nullif(trim(order_id),'') AS order_id, count(*) AS raw_item_count FROM {T('raw','order_items')} GROUP BY 1),
pays AS (
  SELECT order_id, sum(payment_value) AS payment_value, max(payment_installments) AS installments_max,
         count(*) AS payment_records, max_by(payment_type, payment_value) AS payment_type_primary
  FROM {V('payments')} GROUP BY order_id),
rev AS (
  SELECT order_id, review_score FROM (
    SELECT order_id, review_score,
           row_number() OVER (PARTITION BY order_id ORDER BY review_answer_timestamp DESC NULLS LAST, review_id) AS rn
    FROM {V('reviews')}) WHERE rn = 1),
rev_n AS (SELECT order_id, count(*) AS review_records FROM {V('reviews')} GROUP BY order_id),
base AS (
  SELECT o.order_id, xxhash64(c.customer_unique_id) AS customer_key, c.customer_state, c.customer_city, o.order_status,
         o.order_status NOT IN ('canceled','unavailable') AS is_valid_order,
         o.order_status = 'delivered' AS is_delivered,
         o.order_purchase_timestamp AS purchase_ts, to_date(o.order_purchase_timestamp) AS purchase_date,
         trunc(o.order_purchase_timestamp,'MM') AS purchase_month,
         CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
              THEN to_date(o.order_delivered_customer_date) END AS delivered_date,
         to_date(o.order_estimated_delivery_date) AS estimated_date,
         o.order_approved_at, o.order_delivered_carrier_date, o.order_delivered_customer_date, o.order_estimated_delivery_date,
         coalesce(i.items_count,0) AS items_count, coalesce(i.sellers_count,0) AS sellers_count,
         coalesce(r.raw_item_count,0) AS raw_item_count,
         coalesce(i.items_value,0) AS items_value, coalesce(i.freight_value,0) AS freight_value,
         coalesce(i.items_value,0) + coalesce(i.freight_value,0) AS order_value,
         p.payment_value, p.payment_type_primary, p.installments_max,
         v.review_score, v.review_score IS NOT NULL AS has_review, coalesce(n.review_records,0) AS review_records,
         o.dq_warning_count > 0 AS has_dq_warning
  FROM {V('orders')} o
  JOIN cust c ON c.customer_id = o.customer_id
  LEFT JOIN items i ON i.order_id = o.order_id
  LEFT JOIN raw_items r ON r.order_id = o.order_id
  LEFT JOIN pays p ON p.order_id = o.order_id
  LEFT JOIN rev v ON v.order_id = o.order_id
  LEFT JOIN rev_n n ON n.order_id = o.order_id)
SELECT b.*, CAST(date_format(b.purchase_ts,'yyyyMMdd') AS INT) AS purchase_date_key,
       year(b.purchase_ts) AS order_year, month(b.purchase_ts) AS order_month,
       cu.customer_type,
       b.raw_item_count = b.items_count AS is_complete_order,
       b.delivered_date IS NOT NULL AS has_delivery,
       CASE WHEN b.delivered_date IS NOT NULL THEN datediff(b.delivered_date, b.purchase_date) END AS delivery_days,
       datediff(b.estimated_date, b.purchase_date) AS estimated_days,
       CASE WHEN b.delivered_date IS NOT NULL THEN datediff(b.delivered_date, b.estimated_date) END AS delay_days,
       CASE WHEN b.delivered_date IS NULL THEN NULL ELSE b.delivered_date > b.estimated_date END AS is_late,
       CASE WHEN b.delivered_date IS NULL THEN NULL
            WHEN b.delivered_date <= b.estimated_date THEN '1 On time'
            WHEN datediff(b.delivered_date, b.estimated_date) <= 3 THEN '2 Late 1-3 days'
            WHEN datediff(b.delivered_date, b.estimated_date) <= 7 THEN '3 Late 4-7 days'
            ELSE '4 Late 8+ days' END AS delay_bucket,
       CASE WHEN b.review_score IS NULL THEN NULL WHEN b.review_score >= 4 THEN 'Positive (4-5)'
            WHEN b.review_score = 3 THEN 'Neutral (3)' ELSE 'Negative (1-2)' END AS review_group,
       round((unix_timestamp(b.order_approved_at) - unix_timestamp(b.purchase_ts))/3600, 2) AS approval_hours
FROM base b LEFT JOIN {A('dim_customer')} cu ON cu.customer_key = b.customer_key""",
     "Order fact. Grain: one row per valid order. Delivery by calendar date, latest review per order. is_complete_order=false means some of its items were quarantined.")

ctas("fact_order_items", f"""
SELECT i.order_id, i.order_item_id, xxhash64(c.customer_unique_id) AS customer_key, xxhash64(i.product_id) AS product_key,
       xxhash64(i.seller_id) AS seller_key,
       CAST(date_format(o.order_purchase_timestamp,'yyyyMMdd') AS INT) AS purchase_date_key,
       o.order_status, o.order_status NOT IN ('canceled','unavailable') AS is_valid_order,
       i.price, i.freight_value, i.price + i.freight_value AS item_value,
       1.0 / count(*) OVER (PARTITION BY i.order_id) AS order_weight,
       i.shipping_limit_date
FROM {V('order_items')} i
JOIN {V('orders')} o ON i.order_id = o.order_id
JOIN {V('customers')} c ON c.customer_id = o.customer_id""",
     "Sales fact. Grain: one row per valid order item. order_weight = 1/items in order: use it only for averages of order-level fields; never sum order values here.")

ctas("fact_payments", f"""
SELECT p.order_id, p.payment_sequential, xxhash64(c.customer_unique_id) AS customer_key,
       CAST(date_format(o.order_purchase_timestamp,'yyyyMMdd') AS INT) AS purchase_date_key,
       p.payment_type, p.payment_installments, p.payment_value
FROM {V('payments')} p JOIN {V('orders')} o ON p.order_id = o.order_id
JOIN {V('customers')} c ON c.customer_id = o.customer_id""",
     "Payment fact. Grain: one row per valid payment (order_id, payment_sequential).")

ctas("fact_reviews", f"""
SELECT r.review_id, r.order_id, xxhash64(c.customer_unique_id) AS customer_key,
       CAST(date_format(r.review_creation_date,'yyyyMMdd') AS INT) AS review_date_key,
       r.review_score, length(r.review_comment_message) AS comment_length,
       r.review_comment_message IS NOT NULL AS has_comment,
       round((unix_timestamp(r.review_answer_timestamp) - unix_timestamp(r.review_creation_date))/3600, 2) AS response_hours,
       row_number() OVER (PARTITION BY r.order_id ORDER BY r.review_answer_timestamp DESC NULLS LAST, r.review_id) = 1 AS is_latest_review
FROM {V('reviews')} r JOIN {V('orders')} o ON r.order_id = o.order_id
JOIN {V('customers')} c ON c.customer_id = o.customer_id""",
     "Review fact. Grain: one row per valid review on an order. is_latest_review = the single review per order used for scores. Free-text comments are not copied here.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informational PK/FK constraints (feeds the ER view in Catalog Explorer)
# MAGIC Unity Catalog does not enforce these; they document the model. Wrapped in try/except because availability depends on the workspace.

# COMMAND ----------

PKS = {"dim_date": "date_key", "dim_customer": "customer_key", "dim_seller": "seller_key", "dim_product": "product_key",
       "dim_geography": "geography_key", "fact_orders": "order_id", "fact_order_items": "order_id, order_item_id",
       "fact_payments": "order_id, payment_sequential", "fact_reviews": "review_id, order_id"}
FKS = [("fact_order_items", "customer_key", "dim_customer", "customer_key"), ("fact_order_items", "product_key", "dim_product", "product_key"),
       ("fact_order_items", "seller_key", "dim_seller", "seller_key"), ("fact_order_items", "purchase_date_key", "dim_date", "date_key"),
       ("fact_orders", "customer_key", "dim_customer", "customer_key"), ("fact_orders", "purchase_date_key", "dim_date", "date_key"),
       ("fact_payments", "customer_key", "dim_customer", "customer_key"), ("fact_reviews", "customer_key", "dim_customer", "customer_key")]
for t, cols in PKS.items():
    try:
        for c in [x.strip() for x in cols.split(",")]:
            spark.sql(f"ALTER TABLE {A(t)} ALTER COLUMN {c} SET NOT NULL")
        spark.sql(f"ALTER TABLE {A(t)} ADD CONSTRAINT pk_{t} PRIMARY KEY ({cols})")
    except Exception as e:
        print(f"PK skipped for {t}: {str(e)[:120]}")
for ft, fc, dt, dc in FKS:
    try:
        spark.sql(f"ALTER TABLE {A(ft)} ADD CONSTRAINT fk_{ft}_{fc} FOREIGN KEY ({fc}) REFERENCES {A(dt)}({dc})")
    except Exception as e:
        print(f"FK skipped {ft}.{fc}: {str(e)[:120]}")
