# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Defect-injection test (proves the controls work)
# MAGIC The real Olist data is largely clean (your earlier profiling found no orphan keys and no duplicate keys), so on real data most rules
# MAGIC will pass and the quarantine will be small. That is a *finding*, but it does not show that the rules would catch bad data.
# MAGIC
# MAGIC This notebook proves it, **without touching the real pipeline**:
# MAGIC 1. Copies the real raw tables into isolated `*_test` schemas and appends deliberately broken rows (negative price, orphan keys,
# MAGIC    duplicates, impossible dates, invalid status, …), each built from a real row so everything else about it is realistic.
# MAGIC 2. Runs notebooks 03 and 04 on the test schemas (`env_suffix = _test`).
# MAGIC 3. Asserts every injected defect was caught by the **expected rule**, that none of it reached `valid`, that cascades worked (no orphans in valid),
# MAGIC    and that `raw = valid + quarantine` still holds.
# MAGIC 4. Saves the evidence to `governance.dq_injection_test_results`.
# MAGIC
# MAGIC Run after notebook 01 (it needs the real raw tables). Takes a few minutes.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

assert SFX == "", "Run notebook 10 with env_suffix blank; it creates the _test schemas itself."
TEST_SFX = "_test"
TEST_RUN_ID = "TEST" + RUN_ID
R = lambda t: f"{CATALOG}.raw.{t}"
RT = lambda t: f"{CATALOG}.raw{TEST_SFX}.{t}"
VT = lambda t: f"{CATALOG}.valid{TEST_SFX}.{t}"
QT = f"{CATALOG}.quarantine{TEST_SFX}.records"
GT = lambda t: f"{CATALOG}.governance{TEST_SFX}.{t}"
for s_ in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s_}{TEST_SFX}")

BASE = 10**15            # test _row_ids start far above any real _row_id
injected = {t: [] for t in FILES}
EXPECT = []              # (rule_id, table, injected_rows, description)

def newid(tag, col="_row_id"):
    return f"concat('TEST-{tag}-', cast({col} as string))"

def inject(table, rule_id, n, desc, where="1=1", overrides=None):
    off = (len(EXPECT) + 1) * 100000
    df = (spark.table(R(table)).where(where).limit(n)
          .withColumn("_row_id", F.monotonically_increasing_id() + F.lit(BASE + off)))
    for col, expr in (overrides or {}).items():
        df = df.withColumn(col, F.expr(expr))
    injected[table].append(df)
    EXPECT.append((rule_id, table, df.count(), desc))

# fresh, unique item / payment sequence number per injected row: (_row_id - BASE) is unique across ALL batches (offsets differ),
# +1e8 keeps it a plain int. (An earlier "_row_id % 100000" version collided across batches and inflated the duplicate-rule counts.)
seq = f"cast(_row_id - {BASE} + 100000000 as string)"

# ---- customers
inject("customers", "DQ-CUS-02", 6, "exact duplicate customer_id")
inject("customers", "DQ-CUS-03", 10, "invalid state code", overrides={"customer_id": newid("CUS3"), "customer_state": "'XX'"})
inject("customers", "DQ-CUS-04", 5, "zip prefix with 4 digits", overrides={"customer_id": newid("CUS4"), "customer_zip_code_prefix": "'1234'"})
# ---- orders
inject("orders", "DQ-ORD-02", 8, "exact duplicate order_id")
inject("orders", "DQ-ORD-03", 10, "customer does not exist", overrides={"order_id": newid("ORD3"), "customer_id": "'NO-SUCH-CUSTOMER'"})
inject("orders", "DQ-ORD-04", 10, "unknown order status", where="order_status = 'delivered'",
       overrides={"order_id": newid("ORD4"), "order_status": "'teleported'"})
inject("orders", "DQ-ORD-06", 10, "purchase date in the future",
       overrides={"order_id": newid("ORD6"), "order_purchase_timestamp": "'2031-01-01 00:00:00'"})
inject("orders", "DQ-ORD-07", 10, "delivered order without delivery date", where="order_status = 'delivered'",
       overrides={"order_id": newid("ORD7"), "order_delivered_customer_date": "CAST(NULL AS STRING)"})
inject("orders", "DQ-ORD-08", 10, "approved one day before purchase",
       where="order_approved_at IS NOT NULL AND order_purchase_timestamp IS NOT NULL",
       overrides={"order_id": newid("ORD8"),
                  "order_approved_at": "cast(try_cast(order_purchase_timestamp as timestamp) - interval 1 day as string)"})
# ---- order items
inject("order_items", "DQ-ITM-02", 15, "exact duplicate (order_id, order_item_id)")
inject("order_items", "DQ-ITM-04", 20, "item of an order that does not exist",
       overrides={"order_id": newid("ORPH"), "order_item_id": seq})
inject("order_items", "DQ-ITM-05", 10, "item of a product that does not exist",
       overrides={"product_id": "'NO-SUCH-PRODUCT'", "order_item_id": seq})
inject("order_items", "DQ-ITM-08", 25, "negative price (the 'negative fare' case)",
       overrides={"price": "'-12.50'", "order_item_id": seq})
inject("order_items", "DQ-ITM-09", 10, "negative freight", overrides={"freight_value": "'-3.00'", "order_item_id": seq})
# ---- payments
inject("payments", "DQ-PAY-04", 10, "payment_type not_defined", overrides={"payment_type": "'not_defined'", "payment_sequential": seq})
inject("payments", "DQ-PAY-06", 10, "negative payment value", overrides={"payment_value": "'-5.00'", "payment_sequential": seq})
inject("payments", "DQ-PAY-08", 5, "zero installments", overrides={"payment_installments": "'0'", "payment_sequential": seq})
# ---- reviews
inject("reviews", "DQ-REV-03", 5, "review of an order that does not exist",
       overrides={"review_id": newid("REVO"), "order_id": newid("ORPH")})
inject("reviews", "DQ-REV-04", 10, "review score 7", overrides={"review_id": newid("REV4"), "review_score": "'7'"})
# ---- geolocation
inject("geolocation", "DQ-GEO-03", 10, "coordinates outside Brazil (Paris)",
       overrides={"geolocation_lat": "'48.85'", "geolocation_lng": "'2.35'"})

print(f"{len(EXPECT)} injection batches, {sum(e[2] for e in EXPECT)} defective rows")

# COMMAND ----------

# MAGIC %md ## Build the test raw layer = real raw + injected rows

# COMMAND ----------

for t in FILES:
    out = spark.table(R(t))
    for d in injected[t]:
        out = out.unionByName(d)
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RT(t))
    print(f"raw{TEST_SFX}.{t:22s} {spark.table(RT(t)).count():>10,} rows ({sum(1 for e in EXPECT if e[1] == t)} injection batches)")

# COMMAND ----------

# MAGIC %md ## Run the registry and the DQ engine on the test schemas

# COMMAND ----------

args = {"catalog": CATALOG, "env_suffix": TEST_SFX, "run_id": TEST_RUN_ID, "hash_salt_scope": ""}
for step in ["03_dq_rule_registry", "04_dq_engine"]:
    print("▶", step)
    dbutils.notebook.run(f"./{step}", timeout_seconds=3600, arguments=args)

# COMMAND ----------

# MAGIC %md ## Assertions

# COMMAND ----------

results, rows = [], []
def check(name, ok, detail=""):
    results.append((name, bool(ok), str(detail)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))

failed = {r.rule_id: r.failed_count for r in
          spark.table(GT("dq_rule_results")).where(F.col("run_id") == TEST_RUN_ID).collect()}

# 1 · every injected defect is caught by the expected rule
for rule_id, table, n, desc in EXPECT:
    caught = int(failed.get(rule_id, 0))
    ok = caught >= n
    rows.append((TEST_RUN_ID, rule_id, table, desc, n, caught, "CAUGHT" if ok else "MISSED"))
    check(f"{rule_id} caught '{desc}'", ok, f"injected {n}, rule failed {caught}")

# 2 · none of the defects reached the valid layer
def cnt(table, cond): return spark.table(VT(table)).where(cond).count()
check("valid: no non-positive price / negative freight", cnt("order_items", "price <= 0 OR freight_value < 0") == 0)
check("valid: no negative payment, not_defined type or <1 installments",
      cnt("payments", "payment_value < 0 OR payment_type = 'not_defined' OR payment_installments < 1") == 0)
check("valid: no unknown order status", cnt("orders", "order_status = 'teleported'") == 0)
check("valid: no future purchase dates", cnt("orders", "order_purchase_timestamp > current_timestamp()") == 0)
check("valid: no delivered order without delivery date", cnt("orders", "order_status = 'delivered' AND order_delivered_customer_date IS NULL") == 0)
check("valid: no approval before purchase", cnt("orders", "order_approved_at < order_purchase_timestamp") == 0)
check("valid: no order with a missing customer", cnt("orders", "customer_id = 'NO-SUCH-CUSTOMER'") == 0)
check("valid: no review score outside 1-5", cnt("reviews", "review_score NOT BETWEEN 1 AND 5") == 0)
check("valid: no invalid state / 4-digit zip", cnt("customers", "customer_state = 'XX' OR length(customer_zip_code_prefix) <> 5") == 0)
check("valid: no coordinates outside Brazil", cnt("geolocation", "geolocation_lat > 5.5") == 0)

# 3 · cascades: children of rejected parents are rejected, so valid has no orphans
for child, ccol, parent, pcol in [("order_items", "order_id", "orders", "order_id"), ("payments", "order_id", "orders", "order_id"),
                                  ("reviews", "order_id", "orders", "order_id"), ("orders", "customer_id", "customers", "customer_id"),
                                  ("order_items", "product_id", "products", "product_id"), ("order_items", "seller_id", "sellers", "seller_id")]:
    c_df = spark.table(VT(child)).select(F.col(ccol).alias("_ck"))
    p_df = spark.table(VT(parent)).select(F.col(pcol).alias("_pk")).distinct()
    orphans = c_df.join(p_df, c_df["_ck"] == p_df["_pk"], "left_anti").count()
    check(f"valid has no orphan {child}.{ccol} -> {parent}", orphans == 0, orphans)

# 4 · nothing lost: raw = valid + quarantine, on the test copy
qn = {r.source_table: r["count"] for r in spark.table(QT).groupBy("source_table").count().collect()}
for t in PROCESS_ORDER:
    r_, v_, q_ = spark.table(RT(t)).count(), spark.table(VT(t)).count(), qn.get(t, 0)
    check(f"test reconcile raw = valid + quarantine · {t}", r_ == v_ + q_, f"{r_} = {v_} + {q_}")

# 5 · traceability: each quarantine row says why
bad = spark.table(QT).where("size(failed_rules) = 0 OR record_payload IS NULL OR failed_rule_count <> size(failed_rules)").count()
check("every test quarantine row has reasons and the original payload", bad == 0, bad)

# COMMAND ----------

ev = spark.createDataFrame(rows, "test_run_id string, rule_id string, table_name string, defect string, injected_rows long, rule_failed_count long, result string") \
        .withColumn("tested_at", F.current_timestamp())
ev.write.mode("append").saveAsTable(T("governance", "dq_injection_test_results"))
display(ev.orderBy("rule_id"))
display(spark.createDataFrame(results, "check_name string, passed boolean, detail string"))

bad = [r for r in results if not r[1]]
assert not bad, f"{len(bad)} check(s) failed: {[b[0] for b in bad]}"
print(f"All {len(results)} injection-test checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional clean-up (the test schemas are isolated; leave them for screenshots or drop them)
# MAGIC ```sql
# MAGIC DROP SCHEMA <catalog>.raw_test CASCADE;  DROP SCHEMA <catalog>.valid_test CASCADE;  DROP SCHEMA <catalog>.quarantine_test CASCADE;
# MAGIC DROP SCHEMA <catalog>.analytics_test CASCADE;  DROP SCHEMA <catalog>.semantic_test CASCADE;  DROP SCHEMA <catalog>.governance_test CASCADE;
# MAGIC ```
