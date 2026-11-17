# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality engine → `valid` and `quarantine`
# MAGIC For every raw table (parents first):
# MAGIC 1. **Stage** - trim, empty→NULL, `try_cast` to the target type. Nothing is corrected.
# MAGIC 2. **Evaluate** every active rule from `governance.dq_rule_registry` (NULL result = fail).
# MAGIC 3. **Split** - any failed `QUARANTINE` rule → `quarantine.records`; otherwise → `valid.<table>` (with `dq_warning_rules` for `FLAG` rules).
# MAGIC 4. **Measure** - per-rule and per-table counts appended to `governance.dq_rule_results` / `dq_run_summary` (history is kept per run).
# MAGIC
# MAGIC Integrity rules look up the **valid** parent table, so a rejected order automatically rejects its items, payments and reviews
# MAGIC (`DQ-ITM-04`, `DQ-PAY-03`, `DQ-REV-03`). Nothing is deleted: every excluded row is in quarantine with its reasons.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from itertools import chain
from pyspark.sql import functions as F, Window

registry = spark.table(T("governance", "dq_rule_registry")).where("is_active").collect()
assert registry, "Rule registry is empty - run notebook 03 first."
DIM_OF = {r.rule_id: r.dimension for r in registry}
dim_map = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in DIM_OF.items()))

# ---------------------------------------------------------------- staging
def typed_expr(c, typ):
    s = f"nullif(trim(`{c}`), '')"
    if typ == "string":
        return F.expr(s)
    if typ == "int":
        return F.expr(f"try_cast(try_cast({s} as decimal(20,4)) as int)")
    return F.expr(f"try_cast({s} as {typ})")

def stage(table):
    raw = spark.table(T("raw", table))
    cols = []
    for c, typ in STAGING[table].items():
        cols.append(typed_expr(c, typ).alias(c))
        if typ != "string":                                   # keep the cleaned original text to tell 'missing' from 'unparseable'
            cols.append(F.expr(f"nullif(trim(`{c}`), '')").alias(f"{c}__raw"))
    payload = F.to_json(F.struct(*[F.col(f"`{c}`") for c in STAGING[table]]))   # original row, as received
    return raw.select("_row_id", "_source_file", payload.alias("_payload"), *cols)

# ---------------------------------------------------------------- helper columns used by rules
def add_dup_rank(df, keys):
    present = F.expr(" AND ".join(f"`{k}` IS NOT NULL" for k in keys))
    rn = F.row_number().over(Window.partitionBy(*keys).orderBy("_row_id"))
    return df.withColumn("_dup_rank", F.when(present, rn).otherwise(F.lit(1)))

def add_exists(df, flag, left_col, ref_table, ref_col):
    ref = (spark.table(ref_table).select(F.expr(f"nullif(trim(`{ref_col}`), '')").alias("_rk"))
           .where("_rk IS NOT NULL").distinct().withColumn("_rhit", F.lit(True)))
    return (df.join(ref, df[left_col] == ref["_rk"], "left")
              .withColumn(flag, F.coalesce(F.col("_rhit"), F.lit(False))).drop("_rk", "_rhit"))

def order_total(raw_table, expr_sql, alias):
    return (spark.table(T("raw", raw_table))
            .groupBy(F.expr("nullif(trim(order_id), '')").alias("_oid"))
            .agg(F.sum(F.expr(expr_sql)).alias(alias)))

def h_customers(df):
    return add_exists(df, "_zip_in_geo", "customer_zip_code_prefix", T("valid", "geolocation"), "geolocation_zip_code_prefix")

def h_products(df):
    return add_exists(df, "_cat_in_translation", "product_category_name", T("valid", "category_translation"), "product_category_name")

def h_orders(df):
    df = add_exists(df, "_customer_ok", "customer_id", T("valid", "customers"), "customer_id")
    df = add_exists(df, "_has_item_raw", "order_id", T("raw", "order_items"), "order_id")
    items = order_total("order_items", "try_cast(nullif(trim(price),'') as decimal(12,2)) + try_cast(nullif(trim(freight_value),'') as decimal(12,2))", "_items_total")
    pays = order_total("payments", "try_cast(nullif(trim(payment_value),'') as decimal(12,2))", "_pay_total").withColumnRenamed("_oid", "_oid2")
    return (df.join(items, df["order_id"] == items["_oid"], "left").drop("_oid")
              .join(pays, df["order_id"] == pays["_oid2"], "left").drop("_oid2"))

def h_items(df):
    df = add_exists(df, "_order_ok", "order_id", T("valid", "orders"), "order_id")
    df = add_exists(df, "_product_ok", "product_id", T("valid", "products"), "product_id")
    df = add_exists(df, "_seller_ok", "seller_id", T("valid", "sellers"), "seller_id")
    o = spark.table(T("valid", "orders")).select(F.col("order_id").alias("_po"), F.col("order_purchase_timestamp").alias("_order_purchase_ts"))
    return df.join(o, df["order_id"] == o["_po"], "left").drop("_po")

def h_payments(df):
    return add_exists(df, "_order_ok", "order_id", T("valid", "orders"), "order_id")

def h_reviews(df):
    df = add_exists(df, "_order_ok", "order_id", T("valid", "orders"), "order_id")
    return df.withColumn("_reviews_per_order", F.count(F.lit(1)).over(Window.partitionBy("order_id")))

HELPERS = {"customers": h_customers, "products": h_products, "orders": h_orders,
           "order_items": h_items, "payments": h_payments, "reviews": h_reviews}

# ---------------------------------------------------------------- engine
def fcol(rule_id):
    """Per-rule flag column name; hyphens replaced so it is also safe inside SQL strings."""
    return "f__" + rule_id.replace("-", "_")

def failed_array(df, ids):
    if not ids:
        return F.array().cast("array<string>")
    return F.filter(F.array(*[F.when(F.col(fcol(i)), F.lit(i)) for i in ids]), lambda x: x.isNotNull())

def evaluate(table, first_quarantine_write):
    rules = [r for r in registry if r.table_name == table]
    df = add_dup_rank(stage(table), KEYS[table])
    if table in HELPERS:
        df = HELPERS[table](df)

    for r in rules:   # fail = NOT(pass); a NULL pass-condition counts as a failure
        df = df.withColumn(fcol(r.rule_id), ~F.coalesce(F.expr(r.pass_expr), F.lit(False)))

    q_ids = [r.rule_id for r in rules if r.failure_action == "QUARANTINE"]
    w_ids = [r.rule_id for r in rules if r.failure_action == "FLAG"]
    df = (df.withColumn("_failed_q", failed_array(df, q_ids))
            .withColumn("_failed_w", failed_array(df, w_ids))
            .withColumn("_failed_dims", F.array_distinct(F.transform("_failed_q", lambda x: F.element_at(dim_map, x)))))

    # ---- one pass for all counts
    aggs = [F.count(F.lit(1)).alias("n"),
            F.sum((F.size("_failed_q") > 0).cast("int")).alias("quarantined"),
            F.sum(((F.size("_failed_q") == 0) & (F.size("_failed_w") > 0)).cast("int")).alias("valid_with_warnings")]
    aggs += [F.sum(F.col(fcol(r.rule_id)).cast("int")).alias("c__" + r.rule_id.replace("-", "_")) for r in rules]
    res = df.agg(*aggs).first()
    n, nq = int(res["n"]), int(res["quarantined"] or 0)

    # ---- valid layer
    typed_cols = list(STAGING[table].keys())
    valid = (df.where(F.size("_failed_q") == 0)
               .select(*[F.col(f"`{c}`") for c in typed_cols],
                       F.col("_failed_w").alias("dq_warning_rules"), F.size("_failed_w").alias("dq_warning_count"),
                       F.col("_row_id").alias("source_row_id"), F.lit(RUN_ID).alias("dq_run_id")))
    valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("valid", table))

    # ---- quarantine layer (unified table, one row per failed record)
    key_str = F.concat_ws("|", *[F.coalesce(F.col(f"`{k}`").cast("string"), F.lit("<NULL>")) for k in KEYS[table]])
    quarantine = (df.where(F.size("_failed_q") > 0)
                    .select(F.lit(RUN_ID).alias("run_id"), F.lit(table).alias("source_table"), key_str.alias("record_key"),
                            F.col("_failed_q").alias("failed_rules"), F.size("_failed_q").alias("failed_rule_count"),
                            F.col("_failed_dims").alias("failed_dimensions"), F.col("_failed_w").alias("warning_rules"),
                            F.col("_payload").alias("record_payload"), F.col("_source_file").alias("source_file"),
                            F.col("_row_id").alias("source_row_id"), F.current_timestamp().alias("quarantined_at")))
    if first_quarantine_write:      # first table of the run resets the table; the rest append
        quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("quarantine", "records"))
    else:
        quarantine.write.mode("append").saveAsTable(T("quarantine", "records"))

    rule_rows = [(RUN_ID, table, r.rule_id, r.dimension, r.failure_action, n, int(res["c__" + r.rule_id.replace("-", "_")] or 0)) for r in rules]
    summary = (RUN_ID, table, n, n - nq, nq, int(res["valid_with_warnings"] or 0))
    return rule_rows, summary

# COMMAND ----------

all_rules, all_summaries = [], []
for i, tbl in enumerate(PROCESS_ORDER):
    rr, ss = evaluate(tbl, first_quarantine_write=(i == 0))
    all_rules += rr; all_summaries.append(ss)
    print(f"{tbl:22s} evaluated={ss[2]:>9,}  valid={ss[3]:>9,}  quarantined={ss[4]:>7,}  valid_with_warnings={ss[5]:>7,}")

spark.createDataFrame(all_rules, "run_id string, table_name string, rule_id string, dimension string, failure_action string, "
                                 "evaluated_count long, failed_count long") \
     .withColumn("run_ts", F.current_timestamp()).write.mode("append").saveAsTable(T("governance", "dq_rule_results"))
spark.createDataFrame(all_summaries, "run_id string, table_name string, evaluated_count long, valid_count long, "
                                     "quarantined_count long, valid_with_warnings_count long") \
     .withColumn("run_ts", F.current_timestamp()).write.mode("append").saveAsTable(T("governance", "dq_run_summary"))

# COMMAND ----------

# MAGIC %md ### Peek: why were records quarantined? (top rules, then a few sample rows)

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_table, failed_rule, count(*) AS records
  FROM (SELECT source_table, explode(failed_rules) AS failed_rule FROM {T('quarantine','records')})
  GROUP BY 1, 2 ORDER BY records DESC"""))
display(spark.table(T("quarantine", "records")).orderBy(F.desc("failed_rule_count")).limit(20))
