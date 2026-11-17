# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Governance metadata (ownership, glossary, classification, catalog tags, access)
# MAGIC Turns the lecture topics into queryable assets:
# MAGIC
# MAGIC | Lecture topic | Artifact |
# MAGIC |---|---|
# MAGIC | Data domains, owner / steward / custodian / consumer | `governance.data_ownership` |
# MAGIC | RACI | `governance.raci` |
# MAGIC | Business glossary | `governance.business_glossary` |
# MAGIC | Data dictionary / catalog | Unity Catalog table + column **comments** and **tags** (below) |
# MAGIC | Lineage | Automatic in Unity Catalog (Catalog Explorer → table → *Lineage*), plus the query at the bottom |
# MAGIC | Classification (RA 10173 + organisational levels) | `governance.column_classification` + column tags |
# MAGIC | Privacy / access | pseudonymised key in `dim_customer`, comment text excluded from semantic layer, grant + mask templates |
# MAGIC
# MAGIC Roles are placeholders - replace them with real people/groups.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F
G = lambda t: T("governance", t)

def save(rows, schema, name):
    spark.createDataFrame(rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(G(name))
    display(spark.table(G(name)))

# COMMAND ----------

# MAGIC %md ## 1 · Domains and roles

# COMMAND ----------

save([
 ("Customer", "customers", "Head of Customer Operations", "Customer Data Steward", "Data Engineering", "Confidential"),
 ("Seller", "sellers", "Head of Seller Success", "Seller Data Steward", "Data Engineering", "Internal"),
 ("Product", "products, category_translation", "Head of Merchandising", "Product Data Steward", "Data Engineering", "Internal"),
 ("Sales", "orders, order_items", "Head of Sales Operations", "Sales Data Steward", "Data Engineering", "Internal"),
 ("Finance", "payments", "Finance Director", "Finance Data Steward", "Data Engineering", "Confidential"),
 ("Customer Experience", "reviews", "Head of Customer Experience", "CX Data Steward", "Data Engineering", "Confidential"),
 ("Reference Data", "geolocation, category_translation", "Data Governance Lead", "Reference Data Steward", "Data Engineering", "Internal"),
], "data_domain string, source_tables string, data_owner string, data_steward string, data_custodian string, default_classification string",
 "data_ownership")

# COMMAND ----------

# MAGIC %md ## 2 · RACI (A = accountable, R = responsible, C = consulted, I = informed)

# COMMAND ----------

save([
 ("Define 'valid order' / quality expectations", "A", "R", "C", "C"),
 ("Approve rule thresholds and business definitions", "A", "R", "I", "C"),
 ("Implement rules in the pipeline", "C", "C", "R", "I"),
 ("Monitor the data quality dashboard", "A", "R", "C", "C"),
 ("Investigate quarantined records", "I", "A/R", "C", "I"),
 ("Fix a technical defect in the pipeline", "I", "C", "A/R", "I"),
 ("Build and maintain the operational dashboard", "C", "C", "C", "A/R"),
 ("Approve access to Confidential columns", "A", "R", "R", "C"),
], "activity string, data_owner string, data_steward string, data_custodian string, data_consumer string", "raci")

# COMMAND ----------

# MAGIC %md ## 3 · Business glossary (clear definition, owner, status, synonyms)

# COMMAND ----------

save([
 ("Order", "Sales", "A purchase request placed by a customer on the marketplace, identified by order_id. May contain several items.", "Approved", "Head of Sales Operations", "orders.order_id", "purchase", ""),
 ("Valid order", "Sales", "BUSINESS term: an order whose status is neither canceled nor unavailable. Not the same as a valid record (a data-quality term).", "Approved", "Head of Sales Operations", "semantic.orders.is_valid_order", "active order", "order_status NOT IN ('canceled','unavailable')"),
 ("Sales (Items)", "Finance", "Sum of item price, excluding freight, on valid orders. Same definition as the earlier Power BI dashboard.", "Approved", "Finance Director", "semantic.orders.items_value", "revenue, net sales", "SUM(items_value) WHERE is_valid_order"),
 ("Order value", "Finance", "Item price plus freight for an order.", "Approved", "Finance Director", "semantic.orders.order_value", "GMV (per order)", "items_value + freight_value"),
 ("Average order value", "Sales", "Order value (price + freight) divided by the number of valid orders.", "Approved", "Head of Sales Operations", "semantic.orders", "AOV", "SUM(order_value) / COUNT(valid orders)"),
 ("Customer", "Customer", "A real person, identified by customer_unique_id (stored only as a salted hash). NOT the same as customer_id, which is created per order.", "Approved", "Head of Customer Operations", "dim_customer.customer_unique_hash", "unique customer, buyer", "COUNT(DISTINCT customer_hash) WHERE is_valid_order"),
 ("Repeat customer", "Customer", "A customer (person) with two or more valid orders.", "Approved", "Head of Customer Operations", "dim_customer.customer_type", "returning customer", "valid_orders >= 2"),
 ("Delivery time", "Operations", "Calendar days between purchase date and delivered date. Only for delivered orders that have a delivered date.", "Approved", "Head of Sales Operations", "semantic.orders.delivery_days", "lead time", "AVG(delivery_days) WHERE has_delivery"),
 ("Late delivery", "Operations", "A delivered order whose delivered DATE is after the estimated delivery DATE (calendar-day comparison).", "Approved", "Head of Sales Operations", "semantic.orders.is_late", "delayed order", "COUNT(is_late) / COUNT(has_delivery)"),
 ("Review score", "Customer Experience", "Integer 1 (worst) to 5 (best). One review per order is used: the most recent (is_latest_review).", "Approved", "Head of Customer Experience", "semantic.orders.review_score", "CSAT, rating", "AVG(review_score)"),
 ("Valid record", "Data Governance", "A source record that passed every QUARANTINE-level data quality rule. Only valid records feed facts, dimensions and the semantic layer.", "Approved", "Data Governance Lead", "valid.*", "trusted record", ""),
 ("Quarantined record", "Data Governance", "A source record that failed at least one QUARANTINE-level rule. It is retained with the failed rule ids for investigation, never deleted.", "Approved", "Data Governance Lead", "quarantine.records", "rejected record", ""),
 ("Flagged record", "Data Governance", "A valid record that failed a FLAG-level rule. It continues through the pipeline and is counted as a warning.", "Approved", "Data Governance Lead", "valid.*.dq_warning_rules", "warning record", ""),
 ("Data quality pass rate", "Data Governance", "Valid records divided by evaluated records (record level), or passed checks divided by evaluated checks (rule level).", "Approved", "Data Governance Lead", "semantic.dq_trust_scorecard", "DQ score", "valid / evaluated"),
 ("Item value coverage", "Data Governance", "Share of raw order-item value (price + freight) that reached the analytics layer. The headline 'can we trust it?' measure.", "Under review", "Data Governance Lead", "semantic.dq_trust_scorecard", "revenue coverage", "analytics item value / raw item value"),
], "term string, category string, definition string, status string, business_owner string, origin string, synonyms string, formula string",
 "business_glossary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Classification (RA 10173 data privacy classes + organisational levels)
# MAGIC Olist is already anonymised by its publisher, but we treat it as if it were real: the *design* is the exercise.
# MAGIC Public / Internal / Confidential / Restricted follow the lecture; PI / SPI / Privileged follow RA 10173.

# COMMAND ----------

CLASS = [
 ("customers", "customer_id", "PI", "Confidential", "Expose only through hashed key / surrogate key", "Order-scoped identifier"),
 ("customers", "customer_unique_id", "PI", "Confidential", "Pseudonymise (salted SHA-256) before analytics", "Persistent person identifier"),
 ("customers", "customer_zip_code_prefix", "PI", "Confidential", "Stays in valid.customers; not carried into analytics or semantic layers", "Quasi-identifier (location)"),
 ("customers", "customer_city", "None", "Internal", "May be exposed", "Coarse location"),
 ("customers", "customer_state", "None", "Internal", "May be exposed", "Coarse location"),
 ("sellers", "seller_id", "None", "Internal", "May be exposed", "Business entity identifier"),
 ("sellers", "seller_zip_code_prefix", "None", "Internal", "May be exposed", "Business location"),
 ("orders", "order_id", "None", "Internal", "May be exposed", "Transaction identifier"),
 ("payments", "payment_value", "None", "Confidential", "Aggregate in dashboards", "Financial transaction data"),
 ("payments", "payment_type", "None", "Internal", "May be exposed", "Payment method category"),
 ("reviews", "review_comment_title", "PI", "Confidential", "Excluded from semantic layer", "Free text may contain personal data"),
 ("reviews", "review_comment_message", "PI", "Confidential", "Excluded from semantic layer", "Free text may contain personal data"),
 ("reviews", "review_score", "None", "Internal", "May be exposed", "Rating"),
 ("geolocation", "geolocation_lat", "None", "Internal", "Averaged per zip prefix", "Reference coordinates"),
 ("geolocation", "geolocation_lng", "None", "Internal", "Averaged per zip prefix", "Reference coordinates"),
]
save(CLASS, "table_name string, column_name string, personal_data_class string, org_classification string, handling string, rationale string",
     "column_classification")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Unity Catalog comments and tags (the *data dictionary* and *catalog* in practice)
# MAGIC Tags make assets searchable in Catalog Explorer (e.g. filter `domain = Sales`). Wrapped in try/except because tag syntax/availability varies by workspace.

# COMMAND ----------

LAYER = {"raw": "raw", "valid": "valid", "quarantine": "quarantine", "analytics": "analytics", "semantic": "semantic", "governance": "governance"}
DOMAIN_OF = {"customers": "Customer", "customer": "Customer", "sellers": "Seller", "seller": "Seller", "products": "Product", "product": "Product",
             "orders": "Sales", "order_items": "Sales", "payments": "Finance", "reviews": "Customer Experience",
             "geolocation": "Reference Data", "category_translation": "Reference Data"}
OWNER = {r.data_domain: (r.data_owner, r.data_steward) for r in spark.table(G("data_ownership")).collect()}

def tag_table(schema, table, domain, extra=None):
    own, stw = OWNER.get(domain, ("Data Governance Lead", "Reference Data Steward"))
    tags = {"domain": domain, "data_owner": own, "data_steward": stw, "layer": LAYER[schema], **(extra or {})}
    t = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    try:
        spark.sql(f"ALTER TABLE {T(schema, table)} SET TAGS ({t})")
    except Exception as e:
        print(f"tag skipped {schema}.{table}: {str(e)[:100]}")

for t, dom in [("customers", "Customer"), ("sellers", "Seller"), ("products", "Product"), ("orders", "Sales"), ("order_items", "Sales"),
               ("payments", "Finance"), ("reviews", "Customer Experience"), ("geolocation", "Reference Data"), ("category_translation", "Reference Data")]:
    tag_table("raw", t, dom, {"classification": "Confidential" if dom in ("Customer", "Finance", "Customer Experience") else "Internal"})
    tag_table("valid", t, dom)
for t, dom in [("dim_customer", "Customer"), ("dim_seller", "Seller"), ("dim_product", "Product"), ("dim_geography", "Reference Data"),
               ("dim_date", "Reference Data"), ("fact_orders", "Sales"), ("fact_order_items", "Sales"), ("fact_payments", "Finance"),
               ("fact_reviews", "Customer Experience")]:
    tag_table("analytics", t, dom)
tag_table("quarantine", "records", "Reference Data", {"classification": "Confidential", "note": "holds original rows incl. personal data - steward access only"})

# column-level classification tags + comments from the classification table
for r in spark.table(G("column_classification")).collect():
    for schema in ("raw", "valid"):
        try:
            spark.sql(f"ALTER TABLE {T(schema, r.table_name)} ALTER COLUMN {r.column_name} SET TAGS "
                      f"('classification' = '{r.org_classification}', 'personal_data_class' = '{r.personal_data_class}')")
        except Exception as e:
            pass

# glossary-backed comments on the semantic columns that carry metric definitions
COMMENTS = {
 ("orders", "items_value"): "Sum of item price in BRL, excluding freight. Sales (Items) = SUM where is_valid_order.",
 ("orders", "order_value"): "Item price + freight in BRL. AOV = SUM(order_value) / valid orders.",
 ("orders", "customer_hash"): "Salted SHA-256 of customer_unique_id (a person). Count distinct to count real customers.",
 ("orders", "is_valid_order"): "Business term: TRUE unless the order status is canceled or unavailable. Not related to data-quality validity.",
 ("orders", "is_complete_order"): "FALSE when at least one item of the order was quarantined; the order's value is then understated.",
 ("orders", "delivery_days"): "Calendar days from purchase date to delivered date; NULL unless delivered with a delivered date.",
 ("orders", "is_late"): "TRUE if delivered date is after the estimated date; NULL if not delivered.",
 ("orders", "review_score"): "Latest review score (1-5) of the order; NULL if no valid review.",
 ("sales_order_items", "price"): "Item price in BRL, excluding freight.",
 ("sales_order_items", "order_weight"): "1 / items in the order. Use only to average order-level fields (review, late flag) on the item grain.",
}
for (v, c), txt in COMMENTS.items():
    try:
        spark.sql(f"COMMENT ON COLUMN {T('semantic', v)}.{c} IS '{txt}'")
    except Exception as e:
        print(f"comment skipped {v}.{c}: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Access control templates (run only with real groups, as a metastore/catalog admin)
# MAGIC Principle of least privilege, mapped to roles: **analysts** see `semantic` only · **stewards** see `quarantine`, `governance`, `valid` ·
# MAGIC **engineers** manage everything. Quarantine holds raw rows (may contain personal data), so it is never granted to analysts.
# MAGIC Cell is **commented out on purpose**; Free Edition has no groups.

# COMMAND ----------

ACCESS_SQL = f"""
-- groups: data_engineers, data_stewards, bi_analysts
GRANT USE CATALOG ON CATALOG {CATALOG} TO `bi_analysts`, `data_stewards`, `data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.semantic    TO `bi_analysts`, `data_stewards`;
GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.valid       TO `data_stewards`;
GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.quarantine  TO `data_stewards`;
GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.governance  TO `data_stewards`;
GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO `data_engineers`;

-- column mask example: only stewards see the real zip prefix (it is not carried into the analytics layer at all)
CREATE OR REPLACE FUNCTION {CATALOG}.governance.mask_zip(zip STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('data_stewards') THEN zip ELSE '*****' END;
ALTER TABLE {CATALOG}.valid.customers ALTER COLUMN customer_zip_code_prefix SET MASK {CATALOG}.governance.mask_zip;
"""
print(ACCESS_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lineage
# MAGIC Unity Catalog captures table-to-table lineage automatically. Open **Catalog Explorer → `analytics.fact_order_items` → Lineage**
# MAGIC (or the *Lineage graph*) to screenshot raw → valid → fact → semantic for the submission. The query below lists it if system tables are enabled.

# COMMAND ----------

try:
    display(spark.sql(f"""
      SELECT DISTINCT source_table_full_name, target_table_full_name
      FROM system.access.table_lineage
      WHERE target_table_catalog = '{CATALOG}' AND source_table_full_name IS NOT NULL
      ORDER BY target_table_full_name"""))
except Exception as e:
    print("system.access.table_lineage not available here; use the Lineage tab in Catalog Explorer.", str(e)[:100])
