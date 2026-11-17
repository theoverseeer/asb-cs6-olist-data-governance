# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (shared config)
# MAGIC Every other notebook starts with `%run ./00_setup`.
# MAGIC
# MAGIC **Layers (schemas)** - they mirror the case-study architecture:
# MAGIC
# MAGIC | Schema | Purpose |
# MAGIC |---|---|
# MAGIC | `raw` | Untouched source data (all STRING) + ingestion metadata. Also holds the `landing` volume for the CSVs |
# MAGIC | `valid` | Typed records that passed every QUARANTINE rule |
# MAGIC | `quarantine` | Records that failed >= 1 QUARANTINE rule, with the reasons |
# MAGIC | `analytics` | Dimensions and facts, built from `valid` only |
# MAGIC | `semantic` | Business-friendly views for the dashboards (operational + data quality) |
# MAGIC | `governance` | Rule registry, DQ results, glossary, ownership/RACI, classification, profiling output |

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("catalog", "olist_gov", "Catalog")
dbutils.widgets.text("run_id", "", "Run ID (blank = auto)")
dbutils.widgets.text("hash_salt_scope", "", "Secret scope holding 'olist_hash_salt' (optional)")
dbutils.widgets.text("env_suffix", "", "Schema suffix: leave blank for the real run; '_test' is used by notebook 10 only")

CATALOG = dbutils.widgets.get("catalog").strip()
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:  # e.g. workspace does not allow creating catalogs -> use the current one
    CATALOG = spark.sql("SELECT current_catalog()").first()[0]
    print(f"Could not create catalog ({type(e).__name__}); using existing catalog '{CATALOG}'.")
spark.sql(f"USE CATALOG {CATALOG}")

SFX = dbutils.widgets.get("env_suffix").strip()          # '' = real pipeline; '_test' = isolated copy for defect-injection tests
SCHEMAS = ["raw", "valid", "quarantine", "analytics", "semantic", "governance"]
for _s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_s}{SFX}")
if not SFX:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

def T(schema: str, name: str) -> str:
    """Fully qualified table name (schema gets the environment suffix, if any)."""
    return f"{CATALOG}.{schema}{SFX}.{name}"

RUN_ID = dbutils.widgets.get("run_id").strip() or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
LANDING = f"/Volumes/{CATALOG}/raw/landing"

def get_salt() -> str:
    scope = dbutils.widgets.get("hash_salt_scope").strip()
    if scope:
        return dbutils.secrets.get(scope, "olist_hash_salt")
    print("WARNING: using the DEV salt. Create a secret scope and pass hash_salt_scope for real use.")
    return "DEV-ONLY-CHANGE-ME"

# source file -> raw table name
FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

S = "string"
# Typed staging contract: business column -> target type. Raw is ALL string; the DQ engine casts with try_cast
# so a bad value becomes NULL (caught by a rule) instead of crashing the pipeline.
STAGING = {
    "customers": {"customer_id": S, "customer_unique_id": S, "customer_zip_code_prefix": S, "customer_city": S, "customer_state": S},
    "sellers": {"seller_id": S, "seller_zip_code_prefix": S, "seller_city": S, "seller_state": S},
    "products": {"product_id": S, "product_category_name": S, "product_name_lenght": "int", "product_description_lenght": "int",
                 "product_photos_qty": "int", "product_weight_g": "double", "product_length_cm": "double",
                 "product_height_cm": "double", "product_width_cm": "double"},
    "orders": {"order_id": S, "customer_id": S, "order_status": S, "order_purchase_timestamp": "timestamp",
               "order_approved_at": "timestamp", "order_delivered_carrier_date": "timestamp",
               "order_delivered_customer_date": "timestamp", "order_estimated_delivery_date": "timestamp"},
    "order_items": {"order_id": S, "order_item_id": "int", "product_id": S, "seller_id": S, "shipping_limit_date": "timestamp",
                    "price": "decimal(12,2)", "freight_value": "decimal(12,2)"},
    "payments": {"order_id": S, "payment_sequential": "int", "payment_type": S, "payment_installments": "int",
                 "payment_value": "decimal(12,2)"},
    "reviews": {"review_id": S, "order_id": S, "review_score": "int", "review_comment_title": S, "review_comment_message": S,
                "review_creation_date": "timestamp", "review_answer_timestamp": "timestamp"},
    "geolocation": {"geolocation_zip_code_prefix": S, "geolocation_lat": "double", "geolocation_lng": "double",
                    "geolocation_city": S, "geolocation_state": S},
    "category_translation": {"product_category_name": S, "product_category_name_english": S},
}

# Business keys (uniqueness). Geolocation has no business key: many rows per zip are expected by design.
KEYS = {
    "customers": ["customer_id"], "sellers": ["seller_id"], "products": ["product_id"], "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"], "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"], "geolocation": ["_row_id"], "category_translation": ["product_category_name"],
}

# Parents before children, because integrity rules look up the *valid* parent tables.
PROCESS_ORDER = ["category_translation", "geolocation", "customers", "sellers", "products",
                 "orders", "order_items", "payments", "reviews"]

print(f"catalog={CATALOG} run_id={RUN_ID} landing={LANDING} env_suffix={SFX!r}")
