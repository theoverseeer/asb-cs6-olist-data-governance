# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Pipeline assertions (evidence for the grader and a safety net)
# MAGIC Proves the case-study constraints: nothing is lost (`raw = valid + quarantine`), quarantined records never reach the analytics layer,
# MAGIC and the semantic layer does not fan out rows. The notebook **fails loudly** if any check breaks.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok), str(detail)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))

latest = spark.table(T("governance", "dq_run_summary")).agg(F.max("run_id")).first()[0]
summ = {r.table_name: r for r in spark.table(T("governance", "dq_run_summary")).where(F.col("run_id") == latest).collect()}

# 1 · Reconciliation: raw = valid + quarantined, per table
q = spark.table(T("quarantine", "records")).groupBy("source_table").count().collect()
qn = {r.source_table: r["count"] for r in q}
for t in PROCESS_ORDER:
    raw_n, valid_n, q_n = spark.table(T("raw", t)).count(), spark.table(T("valid", t)).count(), qn.get(t, 0)
    check(f"reconcile raw = valid + quarantine · {t}", raw_n == valid_n + q_n, f"{raw_n} = {valid_n} + {q_n}")
    check(f"summary table agrees with real counts · {t}", summ[t].valid_count == valid_n and summ[t].quarantined_count == q_n)

# 2 · Every quarantined record explains itself
bad = spark.table(T("quarantine", "records")).where("size(failed_rules) = 0 OR failed_rule_count <> size(failed_rules) OR record_payload IS NULL").count()
check("every quarantine row has >= 1 failed rule, a count and the original payload", bad == 0, bad)

# 3 · Valid and quarantine are disjoint (by source row), and facts only contain valid keys
#     (compare by source_row_id, not business key: an extra duplicate copy legitimately shares its key with the valid first copy)
for t in PROCESS_ORDER:
    qi = spark.table(T("quarantine", "records")).where(F.col("source_table") == t).select(F.col("source_row_id").alias("r"))
    vi = spark.table(T("valid", t)).select(F.col("source_row_id").alias("r"))
    check(f"no source row is both valid and quarantined · {t}", qi.join(vi, "r").count() == 0)

def not_in_valid(fact, fact_cols, valid_table, valid_cols):
    f = spark.table(T("analytics", fact)).select(*[F.col(c).cast("string").alias(f"k{i}") for i, c in enumerate(fact_cols)])
    v = spark.table(T("valid", valid_table)).select(*[F.col(c).cast("string").alias(f"k{i}") for i, c in enumerate(valid_cols)])
    return f.join(v, [f"k{i}" for i in range(len(fact_cols))], "left_anti").count()
check("fact_orders keys are all in valid.orders", not_in_valid("fact_orders", ["order_id"], "orders", ["order_id"]) == 0)
check("fact_order_items keys are all in valid.order_items",
      not_in_valid("fact_order_items", ["order_id", "order_item_id"], "order_items", ["order_id", "order_item_id"]) == 0)
check("fact_payments keys are all in valid.payments",
      not_in_valid("fact_payments", ["order_id", "payment_sequential"], "payments", ["order_id", "payment_sequential"]) == 0)
check("fact_reviews keys are all in valid.reviews",
      not_in_valid("fact_reviews", ["review_id", "order_id"], "reviews", ["review_id", "order_id"]) == 0)

# 4 · Referential integrity inside the model
A = lambda t: T("analytics", t)
orph = spark.table(A("fact_order_items")).join(spark.table(A("fact_orders")), "order_id", "left_anti").count()
check("every fact_order_items row has its order in fact_orders", orph == 0, orph)
for fk, dim in [("customer_key", "dim_customer"), ("product_key", "dim_product"), ("seller_key", "dim_seller")]:
    n = spark.table(A("fact_order_items")).join(spark.table(A(dim)), fk, "left_anti").count()
    check(f"no orphan {fk} in fact_order_items", n == 0, n)

# 5 · Grain: keys are unique
for t, keys in [("fact_orders", ["order_id"]), ("fact_order_items", ["order_id", "order_item_id"]),
                ("fact_payments", ["order_id", "payment_sequential"]), ("fact_reviews", ["review_id", "order_id"]),
                ("dim_customer", ["customer_key"]), ("dim_product", ["product_key"]), ("dim_seller", ["seller_key"]),
                ("dim_geography", ["geography_key"]), ("dim_date", ["date_key"])]:
    df = spark.table(A(t)); dups = df.count() - df.dropDuplicates(keys).count()
    check(f"grain holds (unique {keys}) · {t}", dups == 0, dups)

# 6 · Business-rule spot checks on the analytics layer (the 'no negative fare' idea)
check("no non-positive item price in fact_order_items", spark.table(A("fact_order_items")).where("price <= 0").count() == 0)
check("no negative freight in fact_order_items", spark.table(A("fact_order_items")).where("freight_value < 0").count() == 0)
check("no negative payment in fact_payments", spark.table(A("fact_payments")).where("payment_value < 0").count() == 0)
check("review scores within 1..5", spark.table(A("fact_reviews")).where("review_score NOT BETWEEN 1 AND 5").count() == 0)
check("no negative delivery time", spark.table(A("fact_orders")).where("delivery_days < 0").count() == 0)

# 7 · Semantic layer does not fan out or drop rows
for view, fact in [("sales_order_items", "fact_order_items"), ("orders", "fact_orders"), ("payments", "fact_payments"), ("reviews", "fact_reviews"), ("customers", "dim_customer")]:
    a, b = spark.table(T("semantic", view)).count(), spark.table(A(fact)).count()
    check(f"semantic.{view} row count equals {fact}", a == b, f"{a} vs {b}")

# 8 · Governance: every active rule was evaluated in the latest run
active = spark.table(T("governance", "dq_rule_registry")).where("is_active").count()
evaluated = spark.table(T("governance", "dq_rule_results")).where(F.col("run_id") == latest).select("rule_id").distinct().count()
check("every active rule has a result in the latest run", active == evaluated, f"{evaluated}/{active}")

summary = spark.createDataFrame(results, "check_name string, passed boolean, detail string")
display(summary)
failed = [r for r in results if not r[1]]
assert not failed, f"{len(failed)} assertion(s) failed: {[r[0] for r in failed]}"
print(f"All {len(results)} assertions passed.")
