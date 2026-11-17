# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Profile the raw tables (justifies the DQ rules)
# MAGIC Covers every item the case study asks for: record counts, missing values, duplicate keys, distinct values of categorical fields,
# MAGIC min/max, date ranges, referential relationships and unusual values.
# MAGIC
# MAGIC **How to use the output:** read each result, decide which findings deserve a rule, then record the decision in
# MAGIC `docs/PROFILING_FINDINGS.md` (a template is in the README). Rules should be traceable to a finding.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

def typed(c, typ):
    s = f"nullif(trim(`{c}`), '')"
    if typ == "string":
        return F.expr(s)
    if typ == "int":   # source stores whole numbers as '58.0' in places; go through decimal first
        return F.expr(f"try_cast(try_cast({s} as decimal(20,4)) as int)")
    return F.expr(f"try_cast({s} as {typ})")

# COMMAND ----------

# MAGIC %md ## 1 · Column profile (rows, nulls, distinct, min, max, unparseable values)

# COMMAND ----------

col_rows = []
for tbl, cols in STAGING.items():
    df = spark.table(T("raw", tbl))
    aggs = [F.count(F.lit(1)).alias("__n")]
    for c, typ in cols.items():
        raw = F.expr(f"nullif(trim(`{c}`), '')")
        t = typed(c, typ)
        aggs += [F.sum(F.when(raw.isNull(), 1).otherwise(0)).alias(f"{c}__nulls"),
                 F.approx_count_distinct(raw).alias(f"{c}__distinct"),
                 F.sum(F.when(raw.isNotNull() & t.isNull(), 1).otherwise(0)).alias(f"{c}__unparseable")]
        if typ != "string":
            aggs += [F.min(t).cast("string").alias(f"{c}__min"), F.max(t).cast("string").alias(f"{c}__max")]
    r = df.agg(*aggs).first().asDict()
    n = r["__n"]
    for c, typ in cols.items():
        col_rows.append((tbl, c, typ, n, int(r[f"{c}__nulls"] or 0), round(100.0 * (r[f"{c}__nulls"] or 0) / max(n, 1), 3),
                         int(r[f"{c}__distinct"] or 0), int(r[f"{c}__unparseable"] or 0),
                         r.get(f"{c}__min"), r.get(f"{c}__max")))

profile = spark.createDataFrame(col_rows, "table_name string, column_name string, target_type string, row_count long, "
                                "null_or_empty long, null_pct double, approx_distinct long, unparseable long, min_value string, max_value string")
profile.withColumn("run_id", F.lit(RUN_ID)).withColumn("profiled_at", F.current_timestamp()) \
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("governance", "profile_columns"))
display(profile.orderBy("table_name", "column_name"))

# COMMAND ----------

# MAGIC %md ## 2 · Duplicate business keys

# COMMAND ----------

dup_rows = []
for tbl, keys in KEYS.items():
    if tbl == "geolocation":
        continue
    df = spark.table(T("raw", tbl)).select(*[F.expr(f"nullif(trim(`{k}`), '')").alias(k) for k in keys])
    g = df.groupBy(*keys).count().where("count > 1")
    dup_keys = g.count()
    extra = g.agg(F.sum(F.col("count") - 1)).first()[0] or 0
    dup_rows.append((tbl, ", ".join(keys), dup_keys, int(extra)))
display(spark.createDataFrame(dup_rows, "table_name string, business_key string, duplicated_keys long, extra_rows long"))

# COMMAND ----------

# MAGIC %md ## 3 · Categorical fields (distinct values and counts)

# COMMAND ----------

for tbl, col in [("orders", "order_status"), ("payments", "payment_type"), ("payments", "payment_installments"),
                 ("reviews", "review_score"), ("customers", "customer_state"), ("sellers", "seller_state"),
                 ("order_items", "order_item_id")]:
    print(f"--- {tbl}.{col}")
    display(spark.table(T("raw", tbl)).groupBy(F.expr(f"nullif(trim(`{col}`), '')").alias(col)).count().orderBy(F.desc("count")).limit(40))

# COMMAND ----------

# MAGIC %md ## 4 · Referential integrity (orphans)

# COMMAND ----------

REFS = [("orders", "customer_id", "customers", "customer_id"),
        ("order_items", "order_id", "orders", "order_id"),
        ("order_items", "product_id", "products", "product_id"),
        ("order_items", "seller_id", "sellers", "seller_id"),
        ("payments", "order_id", "orders", "order_id"),
        ("reviews", "order_id", "orders", "order_id"),
        ("customers", "customer_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
        ("sellers", "seller_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
        ("products", "product_category_name", "category_translation", "product_category_name")]
ref_rows = []
for child, ccol, parent, pcol in REFS:
    c = spark.table(T("raw", child)).select(F.expr(f"nullif(trim(`{ccol}`), '')").alias("k")).where("k is not null")
    p = spark.table(T("raw", parent)).select(F.expr(f"nullif(trim(`{pcol}`), '')").alias("k")).distinct()
    total = c.count()
    orphans = c.join(p, "k", "left_anti").count()
    ref_rows.append((f"{child}.{ccol}", f"{parent}.{pcol}", total, orphans, round(100.0 * orphans / max(total, 1), 3)))
display(spark.createDataFrame(ref_rows, "child string, parent string, child_rows long, orphan_rows long, orphan_pct double"))

# Parents without children (e.g. orders with no items) by status - is it normal for canceled/unavailable?
orders = spark.table(T("raw", "orders")).select("order_id", "order_status")
items = spark.table(T("raw", "order_items")).select("order_id").distinct().withColumn("has_item", F.lit(1))
display(orders.join(items, "order_id", "left").groupBy("order_status")
        .agg(F.count("*").alias("orders"), F.sum(F.when(F.col("has_item").isNull(), 1).otherwise(0)).alias("orders_without_items")))

# COMMAND ----------

# MAGIC %md ## 5 · Unusual values worth a rule (negatives, zeros, impossible dates)

# COMMAND ----------

o = spark.table(T("raw", "orders")).select(*[typed(c, t).alias(c) for c, t in STAGING["orders"].items()])
display(o.agg(
    F.min("order_purchase_timestamp").alias("first_purchase"), F.max("order_purchase_timestamp").alias("last_purchase"),
    F.sum(F.when(F.col("order_approved_at") < F.col("order_purchase_timestamp"), 1).otherwise(0)).alias("approved_before_purchase"),
    F.sum(F.when(F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"), 1).otherwise(0)).alias("delivered_before_purchase"),
    F.sum(F.when(F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"), 1).otherwise(0)).alias("delivered_before_carrier"),
    F.sum(F.when((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull(), 1).otherwise(0)).alias("delivered_without_delivery_date"),
    F.sum(F.when((F.col("order_status") != "delivered") & F.col("order_delivered_customer_date").isNotNull(), 1).otherwise(0)).alias("not_delivered_but_has_date")))

i = spark.table(T("raw", "order_items")).select(*[typed(c, t).alias(c) for c, t in STAGING["order_items"].items()])
display(i.agg(F.min("price").alias("min_price"), F.max("price").alias("max_price"),
              F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("price_le_zero"),
              F.min("freight_value").alias("min_freight"), F.max("freight_value").alias("max_freight"),
              F.sum(F.when(F.col("freight_value") < 0, 1).otherwise(0)).alias("negative_freight")))

p = spark.table(T("raw", "payments")).select(*[typed(c, t).alias(c) for c, t in STAGING["payments"].items()])
display(p.agg(F.min("payment_value").alias("min_payment"), F.max("payment_value").alias("max_payment"),
              F.sum(F.when(F.col("payment_value") < 0, 1).otherwise(0)).alias("negative_payments"),
              F.sum(F.when(F.col("payment_value") == 0, 1).otherwise(0)).alias("zero_payments"),
              F.sum(F.when(F.col("payment_installments") < 1, 1).otherwise(0)).alias("installments_lt_1"),
              F.max("payment_installments").alias("max_installments")))

g = spark.table(T("raw", "geolocation")).select(*[typed(c, t).alias(c) for c, t in STAGING["geolocation"].items()])
display(g.agg(F.count("*").alias("rows"), F.countDistinct("geolocation_zip_code_prefix").alias("distinct_zips"),
              F.sum(F.when(~F.col("geolocation_lat").between(-34, 5.5) | ~F.col("geolocation_lng").between(-74, -34), 1).otherwise(0)).alias("outside_brazil_bbox")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Reconcile payments with items (cross-dataset consistency)
# MAGIC Shows how many orders disagree and by how much; this is what justifies `DQ-ORD-14` and its tolerance.

# COMMAND ----------

it = (spark.table(T("raw", "order_items")).groupBy(F.expr("nullif(trim(order_id),'')").alias("order_id"))
      .agg(F.sum(typed("price", "decimal(12,2)") + typed("freight_value", "decimal(12,2)")).alias("items_total")))
pa = (spark.table(T("raw", "payments")).groupBy(F.expr("nullif(trim(order_id),'')").alias("order_id"))
      .agg(F.sum(typed("payment_value", "decimal(12,2)")).alias("pay_total")))
rec = it.join(pa, "order_id", "full").withColumn("diff", F.abs(F.coalesce("pay_total", F.lit(0)) - F.coalesce("items_total", F.lit(0))))
display(rec.agg(F.count("*").alias("orders_compared"),
                F.sum(F.when(F.col("diff") > 1, 1).otherwise(0)).alias("diff_gt_1_brl"),
                F.sum(F.when(F.col("diff") > 0.05 * F.coalesce("items_total", F.lit(0)), 1).otherwise(0)).alias("diff_gt_5pct"),
                F.max("diff").alias("max_diff")))
