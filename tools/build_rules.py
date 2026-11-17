import json, re, os, textwrap, collections

import pathlib
OUT = str(pathlib.Path(__file__).resolve().parent.parent)   # repo root
os.makedirs(f"{OUT}/notebooks", exist_ok=True)
os.makedirs(f"{OUT}/docs", exist_ok=True)

UF = ("AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI",
      "RJ","RN","RS","RO","RR","SC","SP","SE","TO")
UF_SQL = ",".join(f"'{u}'" for u in UF)

# table -> (domain, owner_role, steward_role)
DOMAIN = {
    "customers": ("Customer", "Head of Customer Operations", "Customer Data Steward"),
    "sellers": ("Seller", "Head of Seller Success", "Seller Data Steward"),
    "products": ("Product", "Head of Merchandising", "Product Data Steward"),
    "orders": ("Sales", "Head of Sales Operations", "Sales Data Steward"),
    "order_items": ("Sales", "Head of Sales Operations", "Sales Data Steward"),
    "payments": ("Finance", "Finance Director", "Finance Data Steward"),
    "reviews": ("Customer Experience", "Head of Customer Experience", "CX Data Steward"),
    "geolocation": ("Reference Data", "Data Governance Lead", "Reference Data Steward"),
    "category_translation": ("Reference Data", "Data Governance Lead", "Reference Data Steward"),
}

Q, F = "QUARANTINE", "FLAG"
RULES = []
def R(rid, dim, table, field, desc, expr, why, action=Q, target=99.9):
    RULES.append(dict(rule_id=rid, dimension=dim, table_name=table, field=field,
                      rule_description=desc, pass_expr=expr, business_rationale=why,
                      failure_action=action, target_pass_rate=float(target)))

# ---------------- customers
R("DQ-CUS-01","Completeness","customers","customer_id, customer_unique_id",
  "customer_id and customer_unique_id must be present.",
  "customer_id IS NOT NULL AND customer_unique_id IS NOT NULL",
  "customer_id is the join key to orders; customer_unique_id is used to count real (repeat) customers.", Q, 100)
R("DQ-CUS-02","Uniqueness","customers","customer_id",
  "customer_id must appear once (first occurrence kept, extra copies quarantined).",
  "_dup_rank = 1",
  "Duplicate keys fan out joins and double-count revenue.", Q, 100)
R("DQ-CUS-03","Validity","customers","customer_state",
  "customer_state must be one of the 27 Brazilian UF codes.",
  f"customer_state IN ({UF_SQL})",
  "State is a core geographic dimension; invalid codes break regional KPIs and maps.")
R("DQ-CUS-04","Validity","customers","customer_zip_code_prefix",
  "customer_zip_code_prefix must be exactly 5 digits (read as text, so leading zeros survive).",
  "customer_zip_code_prefix RLIKE '^[0-9]{5}$'",
  "Zip prefix is the join key to geolocation; malformed values cannot be mapped.")
R("DQ-CUS-05","Integrity","customers","customer_zip_code_prefix -> geolocation",
  "customer_zip_code_prefix should exist in valid geolocation reference data.",
  "_zip_in_geo",
  "Only affects map visuals, not sales correctness, so the record is flagged and kept.", F, 98)

# ---------------- sellers
R("DQ-SEL-01","Completeness","sellers","seller_id","seller_id must be present.",
  "seller_id IS NOT NULL","Seller performance cannot be attributed without a seller key.", Q, 100)
R("DQ-SEL-02","Uniqueness","sellers","seller_id","seller_id must appear once.",
  "_dup_rank = 1","Duplicate sellers double-count seller revenue.", Q, 100)
R("DQ-SEL-03","Validity","sellers","seller_state","seller_state must be a valid UF code.",
  f"seller_state IN ({UF_SQL})","Seller geography drives regional supply analysis.")
R("DQ-SEL-04","Validity","sellers","seller_zip_code_prefix","seller_zip_code_prefix must be exactly 5 digits.",
  "seller_zip_code_prefix RLIKE '^[0-9]{5}$'","Needed to map sellers to geography.")

# ---------------- products
R("DQ-PRD-01","Completeness","products","product_id","product_id must be present.",
  "product_id IS NOT NULL","Product key is required to analyse product and category performance.", Q, 100)
R("DQ-PRD-02","Uniqueness","products","product_id","product_id must appear once.",
  "_dup_rank = 1","Duplicate products double-count item revenue.", Q, 100)
R("DQ-PRD-03","Completeness","products","product_category_name","Product should have a category.",
  "product_category_name IS NOT NULL",
  "Missing category hides revenue under 'unknown'; sales are still real, so flag and keep.", F, 98)
R("DQ-PRD-04","Integrity","products","product_category_name -> category_translation",
  "When present, the category must exist in the translation reference table.",
  "product_category_name IS NULL OR _cat_in_translation",
  "Untranslated categories show Portuguese labels in the dashboard.", F, 99)
R("DQ-PRD-05","Reasonability","products","product_weight_g, length/height/width_cm",
  "Weight and all three dimensions must be greater than zero.",
  "product_weight_g > 0 AND product_length_cm > 0 AND product_height_cm > 0 AND product_width_cm > 0",
  "A physical product cannot weigh 0 g or have 0 cm dimensions; affects freight analysis only, so flag and keep.", F, 99)

# ---------------- orders
R("DQ-ORD-01","Completeness","orders","order_id, customer_id, order_status, order_purchase_timestamp",
  "Order key, customer key, status and purchase timestamp must be present.",
  "order_id IS NOT NULL AND customer_id IS NOT NULL AND order_status IS NOT NULL AND order_purchase_timestamp IS NOT NULL",
  "These fields anchor every order, customer and time-based metric.", Q, 100)
R("DQ-ORD-02","Uniqueness","orders","order_id","order_id must appear once.",
  "_dup_rank = 1","Duplicate orders inflate order counts and revenue.", Q, 100)
R("DQ-ORD-03","Integrity","orders","customer_id -> valid customers",
  "customer_id must exist in the valid customers table.",
  "_customer_ok","An order with no valid customer cannot be attributed to a customer or region.")
R("DQ-ORD-04","Validity","orders","order_status",
  "order_status must be one of the 8 known lifecycle statuses.",
  "order_status IN ('created','approved','invoiced','processing','shipped','delivered','unavailable','canceled')",
  "Unknown statuses make revenue/delivery filters unreliable.", Q, 100)
R("DQ-ORD-05","Validity","orders","all order timestamps",
  "Every timestamp that is filled in must be a parseable date-time.",
  "(order_purchase_timestamp__raw IS NULL OR order_purchase_timestamp IS NOT NULL) AND (order_approved_at__raw IS NULL OR order_approved_at IS NOT NULL) AND (order_delivered_carrier_date__raw IS NULL OR order_delivered_carrier_date IS NOT NULL) AND (order_delivered_customer_date__raw IS NULL OR order_delivered_customer_date IS NOT NULL) AND (order_estimated_delivery_date__raw IS NULL OR order_estimated_delivery_date IS NOT NULL)",
  "Unparseable dates silently become NULL downstream and corrupt delivery metrics.")
R("DQ-ORD-06","Reasonability","orders","order_purchase_timestamp",
  "Purchase date must be between 2016-01-01 and now.",
  "order_purchase_timestamp >= TIMESTAMP'2016-01-01 00:00:00' AND order_purchase_timestamp <= current_timestamp()",
  "Orders before the marketplace existed or in the future are impossible (lower bound is a stated assumption; confirm with the Data Owner).")
R("DQ-ORD-07","Completeness","orders","order_delivered_customer_date, order_approved_at",
  "A delivered order must have both an approval date and a customer delivery date.",
  "order_status <> 'delivered' OR (order_delivered_customer_date IS NOT NULL AND order_approved_at IS NOT NULL)",
  "A delivered order without delivery date breaks delivery-time and on-time KPIs.")
R("DQ-ORD-08","Consistency","orders","order_approved_at vs order_purchase_timestamp",
  "Approval cannot happen before purchase.",
  "order_approved_at IS NULL OR order_approved_at >= order_purchase_timestamp",
  "Time travel in the order lifecycle signals a bad timestamp.")
R("DQ-ORD-09","Consistency","orders","order_delivered_customer_date vs order_purchase_timestamp",
  "Customer delivery cannot happen before purchase.",
  "order_delivered_customer_date IS NULL OR order_delivered_customer_date >= order_purchase_timestamp",
  "Negative delivery times would corrupt average delivery time (same spirit as the negative-fare example in the demo).")
R("DQ-ORD-10","Consistency","orders","carrier date sequence",
  "Carrier handover must not precede purchase, and customer delivery must not precede carrier handover.",
  "(order_delivered_carrier_date IS NULL OR order_delivered_carrier_date >= order_purchase_timestamp) AND (order_delivered_carrier_date IS NULL OR order_delivered_customer_date IS NULL OR order_delivered_customer_date >= order_delivered_carrier_date)",
  "Logistics timestamps are often entered manually; flagged for the steward rather than dropping otherwise-good orders.", F, 99)
R("DQ-ORD-11","Consistency","orders","order_delivered_customer_date vs order_status",
  "Only delivered orders should carry a customer delivery date.",
  "order_status = 'delivered' OR order_delivered_customer_date IS NULL",
  "Status and dates contradict each other; needs investigation but revenue logic uses status.", F, 99.5)
R("DQ-ORD-12","Consistency","orders","order_estimated_delivery_date",
  "Estimated delivery date must exist and not precede purchase.",
  "order_estimated_delivery_date >= order_purchase_timestamp",
  "On-time delivery is measured against this promise.", F, 99.5)
R("DQ-ORD-13","Integrity","orders","order_id -> order_items",
  "A delivered order must have at least one order-item record.",
  "order_status <> 'delivered' OR _has_item_raw",
  "A delivered order with no items has no sales value (canceled/unavailable orders may legitimately have none).")
R("DQ-ORD-14","Consistency","orders","payments vs items total",
  "Total payment value should match items + freight within max(1.00 BRL, 5%).",
  "order_status IN ('canceled','unavailable') OR abs(coalesce(_pay_total,0) - coalesce(_items_total,0)) <= greatest(1.00, 0.05 * coalesce(_items_total,0))",
  "Cross-dataset reconciliation (same idea as 'CRM vs billing' in the lecture). Installment interest explains small gaps, so tolerance is a steward-owned assumption.", F, 98)

# ---------------- order_items
R("DQ-ITM-01","Completeness","order_items","order_id, order_item_id, product_id, seller_id",
  "All four key columns must be present.",
  "order_id IS NOT NULL AND order_item_id IS NOT NULL AND product_id IS NOT NULL AND seller_id IS NOT NULL",
  "The item row is the grain of the sales fact; every key is needed to join dimensions.", Q, 100)
R("DQ-ITM-02","Uniqueness","order_items","(order_id, order_item_id)",
  "The composite key must appear once.","_dup_rank = 1","Duplicate items double-count revenue.", Q, 100)
R("DQ-ITM-03","Validity","order_items","order_item_id","order_item_id must be 1 or greater.",
  "order_item_id >= 1","Item sequence starts at 1.")
R("DQ-ITM-04","Integrity","order_items","order_id -> valid orders","Parent order must be a valid order.",
  "_order_ok","Prevents orphan items and cascades order-level failures so facts stay consistent.")
R("DQ-ITM-05","Integrity","order_items","product_id -> valid products","Product must be a valid product.",
  "_product_ok","Item cannot be attributed to a product/category otherwise.")
R("DQ-ITM-06","Integrity","order_items","seller_id -> valid sellers","Seller must be a valid seller.",
  "_seller_ok","Item cannot be attributed to a seller otherwise.")
R("DQ-ITM-07","Validity","order_items","price, freight_value","price and freight_value must be present and numeric.",
  "price IS NOT NULL AND freight_value IS NOT NULL","Revenue is computed from these two columns.", Q, 100)
R("DQ-ITM-08","Reasonability","order_items","price","price must be greater than 0 and at most 10,000 BRL.",
  "price > 0 AND price <= 10000",
  "Customers do not pay negative or zero prices (the negative-fare lesson from the demo). Upper bound is a steward-owned assumption.")
R("DQ-ITM-09","Reasonability","order_items","freight_value","freight_value must be between 0 and 1,000 BRL.",
  "freight_value >= 0 AND freight_value <= 1000",
  "Free shipping (0) is legitimate; negative or extreme freight is not.")
R("DQ-ITM-10","Consistency","order_items","shipping_limit_date vs order purchase",
  "When present, the seller shipping deadline must not precede order purchase.",
  "shipping_limit_date IS NULL OR _order_purchase_ts IS NULL OR shipping_limit_date >= _order_purchase_ts",
  "Does not affect revenue; flagged for seller-operations review.", F, 99.5)

# ---------------- payments
R("DQ-PAY-01","Completeness","payments","order_id, payment_sequential","Both key columns must be present.",
  "order_id IS NOT NULL AND payment_sequential IS NOT NULL","Payment rows must be traceable to an order.", Q, 100)
R("DQ-PAY-02","Uniqueness","payments","(order_id, payment_sequential)","The composite key must appear once.",
  "_dup_rank = 1","Duplicate payments double-count collected money.", Q, 100)
R("DQ-PAY-03","Integrity","payments","order_id -> valid orders","Parent order must be a valid order.",
  "_order_ok","Payments for rejected orders must not reach the analytics layer.")
R("DQ-PAY-04","Validity","payments","payment_type","payment_type must be credit_card, boleto, voucher or debit_card.",
  "payment_type IN ('credit_card','boleto','voucher','debit_card')",
  "Anything else (e.g. 'not_defined') cannot be analysed in the payment-mix report.")
R("DQ-PAY-05","Validity","payments","payment_value","payment_value must be present and numeric.",
  "payment_value IS NOT NULL","Finance metrics rely on this value.", Q, 100)
R("DQ-PAY-06","Reasonability","payments","payment_value","payment_value must not be negative.",
  "payment_value >= 0","Customers do not pay negative amounts (negative-fare lesson).", Q, 100)
R("DQ-PAY-07","Reasonability","payments","payment_value","payment_value should be greater than zero.",
  "payment_value > 0","Zero-value payments can be legitimate (e.g. full voucher), so flag for the steward and keep.", F, 99.5)
R("DQ-PAY-08","Validity","payments","payment_installments","payment_installments must be 1 or greater.",
  "payment_installments >= 1","A payment cannot be split into zero installments.")
R("DQ-PAY-09","Reasonability","payments","payment_installments","payment_installments should not exceed 24.",
  "payment_installments IS NULL OR payment_installments <= 24",
  "Unusually long plans need confirmation (24 is an assumption; confirm with Finance).", F, 99.5)

# ---------------- reviews
R("DQ-REV-01","Completeness","reviews","review_id, order_id","Both key columns must be present.",
  "review_id IS NOT NULL AND order_id IS NOT NULL","A review must be traceable to an order.", Q, 100)
R("DQ-REV-02","Uniqueness","reviews","(review_id, order_id)",
  "The pair must appear once (the same review_id legitimately appears on different orders).",
  "_dup_rank = 1","Exact duplicates double-weight satisfaction scores.", Q, 100)
R("DQ-REV-03","Integrity","reviews","order_id -> valid orders","Parent order must be a valid order.",
  "_order_ok","Reviews of rejected orders must not reach the analytics layer.")
R("DQ-REV-04","Validity","reviews","review_score","review_score must be an integer from 1 to 5.",
  "review_score BETWEEN 1 AND 5","Satisfaction scale is 1-5; other values distort averages.", Q, 100)
R("DQ-REV-05","Completeness","reviews","review_creation_date","review_creation_date must be present and parseable.",
  "review_creation_date IS NOT NULL","Needed to place the review in time.", Q, 100)
R("DQ-REV-06","Consistency","reviews","review_answer_timestamp vs review_creation_date",
  "Review answer cannot precede review creation.",
  "review_answer_timestamp IS NULL OR review_answer_timestamp >= review_creation_date",
  "Affects response-time metric only.", F, 99.5)
R("DQ-REV-07","Uniqueness","reviews","order_id","Each order should have only one review.",
  "_reviews_per_order = 1","Multiple reviews per order complicate satisfaction-per-order metrics; flagged, not removed.", F, 99)

# ---------------- geolocation
R("DQ-GEO-01","Completeness","geolocation","zip, lat, lng","Zip prefix, latitude and longitude must be present.",
  "geolocation_zip_code_prefix IS NOT NULL AND geolocation_lat IS NOT NULL AND geolocation_lng IS NOT NULL",
  "A coordinate row without a zip or coordinates is useless.")
R("DQ-GEO-02","Validity","geolocation","geolocation_zip_code_prefix","Zip prefix must be exactly 5 digits.",
  "geolocation_zip_code_prefix RLIKE '^[0-9]{5}$'","Join key to customers/sellers.")
R("DQ-GEO-03","Reasonability","geolocation","geolocation_lat, geolocation_lng",
  "Coordinates must fall inside Brazil's bounding box (lat -34 to 5.5, lng -74 to -34).",
  "geolocation_lat BETWEEN -34 AND 5.5 AND geolocation_lng BETWEEN -74 AND -34",
  "Points outside Brazil would distort maps and averaged coordinates.", Q, 99.5)
R("DQ-GEO-04","Validity","geolocation","geolocation_state","State must be a valid UF code.",
  f"geolocation_state IN ({UF_SQL})","Consistent state codes across all reference data.")

# ---------------- category translation
R("DQ-CAT-01","Completeness","category_translation","both columns","Portuguese and English names must be present.",
  "product_category_name IS NOT NULL AND product_category_name_english IS NOT NULL",
  "A half-filled mapping row cannot translate anything.", Q, 100)
R("DQ-CAT-02","Uniqueness","category_translation","product_category_name","Each Portuguese category appears once.",
  "_dup_rank = 1","Duplicates would fan out the product dimension.", Q, 100)

# ------------------------------------------------------------- static validation
STAGING = {
 "customers": ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
 "sellers": ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
 "products": ["product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
 "orders": ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
 "order_items": ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
 "payments": ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
 "reviews": ["review_id","order_id","review_score","review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp"],
 "geolocation": ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"],
 "category_translation": ["product_category_name","product_category_name_english"],
}
HELPERS = {
 "customers": {"_zip_in_geo"}, "sellers": set(), "products": {"_cat_in_translation"},
 "orders": {"_customer_ok","_has_item_raw","_items_total","_pay_total"},
 "order_items": {"_order_ok","_product_ok","_seller_ok","_order_purchase_ts"},
 "payments": {"_order_ok"}, "reviews": {"_order_ok","_reviews_per_order"},
 "geolocation": set(), "category_translation": set(),
}
SQLWORDS = set("""is not null and or in between rlike abs coalesce greatest current_timestamp timestamp
case when then else end""".split())

ids = [r["rule_id"] for r in RULES]
assert len(ids) == len(set(ids)), "duplicate rule ids"
problems = []
for r in RULES:
    expr = re.sub(r"'[^']*'", "''", r["pass_expr"])           # drop string literals
    expr = re.sub(r"\b\d+(\.\d+)?\b", "", expr)
    allowed = set(STAGING[r["table_name"]]) | {c + "__raw" for c in STAGING[r["table_name"]]} \
              | HELPERS[r["table_name"]] | {"_dup_rank"} | SQLWORDS
    for tok in set(re.findall(r"[A-Za-z_][A-Za-z_0-9]*", expr)):
        if tok.lower() not in {a.lower() for a in allowed}:
            problems.append((r["rule_id"], tok))
    assert r["table_name"] in DOMAIN
print("static check -> unknown tokens:", problems)
assert not problems

for r in RULES:
    r["data_domain"], r["owner_role"], r["steward_role"] = DOMAIN[r["table_name"]]
    r["is_active"] = True
    r["rule_version"] = 1

# ------------------------------------------------------------- notebook 03
nb = ["# Databricks notebook source",
"# MAGIC %md",
"# MAGIC # 03 · Data quality rule registry",
"# MAGIC",
"# MAGIC Rules are **data, not code**: each rule is a row with an owner, a steward, a dimension, a pass-condition (Spark SQL),",
"# MAGIC a failure action and a target pass-rate. The engine (notebook 04) reads this table, so adding or retiring a rule never means editing pipeline code.",
"# MAGIC",
"# MAGIC * `QUARANTINE` = record is removed from the valid layer and kept in `quarantine.records` with the reason.",
"# MAGIC * `FLAG` = record stays valid but is tagged (`dq_warning_rules`) and counted on the scorecard.",
"# MAGIC",
"# MAGIC > Generated by `tools/build_rules.py` (single source of truth for the rules and `docs/DQ_RULES.md`). Governance owners (Data Owner / Steward) are *roles*; replace with real names in your org.",
"",
"# COMMAND ----------",
"",
"# MAGIC %run ./00_setup",
"",
"# COMMAND ----------",
"",
"import json",
"from pyspark.sql import functions as F",
"from pyspark.sql.types import *",
"",
"RULES = json.loads(r'''" + json.dumps(RULES, indent=1) + "''')",
"",
"schema = StructType([",
"    StructField('rule_id', StringType()), StructField('dimension', StringType()),",
"    StructField('table_name', StringType()), StructField('field', StringType()),",
"    StructField('rule_description', StringType()), StructField('pass_expr', StringType()),",
"    StructField('business_rationale', StringType()), StructField('failure_action', StringType()),",
"    StructField('target_pass_rate', DoubleType()), StructField('data_domain', StringType()),",
"    StructField('owner_role', StringType()), StructField('steward_role', StringType()),",
"    StructField('is_active', BooleanType()), StructField('rule_version', IntegerType()),",
"])",
"df = (spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in RULES], schema)",
"        .withColumn('custodian_role', F.lit('Data Engineering'))",
"        .withColumn('registered_at', F.current_timestamp()))",
"df.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(T('governance', 'dq_rule_registry'))",
"display(spark.table(T('governance','dq_rule_registry')).groupBy('table_name','failure_action').count().orderBy('table_name'))",
"",
"# COMMAND ----------",
"",
"# MAGIC %md",
"# MAGIC ### Smoke test: every rule expression must parse in Spark SQL",
"# MAGIC (Catches typos before the engine runs. Unknown columns are expected here because we test against an empty dummy row, so we only check *syntax*.)",
"",
"# COMMAND ----------",
"",
"bad = []",
"for r in RULES:",
"    try:",
"        spark.sql(f\"SELECT ({r['pass_expr']}) AS ok\").schema  # unresolved columns raise AnalysisException; syntax errors raise ParseException",
"    except Exception as e:",
"        if 'ParseException' in type(e).__name__ or 'PARSE_SYNTAX_ERROR' in str(e):",
"            bad.append((r['rule_id'], str(e)[:200]))",
"assert not bad, bad",
"print(f'{len(RULES)} rules registered; all parse.')",
]
open(f"{OUT}/notebooks/03_dq_rule_registry.py", "w").write("\n".join(nb) + "\n")

# ------------------------------------------------------------- docs/DQ_RULES.md
by_table = collections.OrderedDict()
for r in RULES: by_table.setdefault(r["table_name"], []).append(r)
nq = sum(r["failure_action"] == Q for r in RULES); nf = len(RULES) - nq
dims = collections.Counter(r["dimension"] for r in RULES)
md = ["# Data quality rules", "",
      f"**{len(RULES)} rules** over 9 source tables: **{nq} QUARANTINE** (record is isolated) and **{nf} FLAG** (record continues, warning is logged).",
      "Dimensions covered: " + ", ".join(f"{k} ({v})" for k, v in sorted(dims.items())) + ".", "",
      "**Failure actions**", "",
      "* `QUARANTINE` - the record never reaches `valid.*`; it is stored in `quarantine.records` with `failed_rules`, `failed_dimensions`, `failed_rule_count` and the original row as JSON.",
      "* `FLAG` - the record stays in `valid.*` with the rule id in `dq_warning_rules`; used where the defect does not invalidate the business fact.",
      "* No value is auto-corrected. The only transformations are trimming whitespace, turning empty strings into NULL, and type casting (`try_cast`); anything that fails to cast is caught by a rule.", "",
      "**Why Timeliness and Accuracy are not implemented:** the Olist extract is a static historical snapshot (no freshness SLA applies), and there is no independent ground truth to measure accuracy against. Both are documented as known limitations.", "",
      "The executable pass-conditions (Spark SQL) live in `notebooks/03_dq_rule_registry.py` / table `governance.dq_rule_registry`; this file is generated from the same list.", ""]
for t, rs in by_table.items():
    d, o, s = DOMAIN[t]
    md += [f"## `{t}`  ·  domain: {d}  ·  owner: {o}  ·  steward: {s}", "",
           "| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |",
           "|---|---|---|---|---|---|---|"]
    for r in rs:
        esc = lambda x: x.replace("|", "\\|")
        md.append(f"| {r['rule_id']} | {r['dimension']} | `{t}`.{esc(r['field'])} | {esc(r['rule_description'])} | {esc(r['business_rationale'])} | {r['failure_action']} | {r['target_pass_rate']:g}% |")
    md.append("")
open(f"{OUT}/docs/DQ_RULES.md", "w").write("\n".join(md))
print("rules:", len(RULES), "quarantine:", nq, "flag:", nf, dict(dims))
