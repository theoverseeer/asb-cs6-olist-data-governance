# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Run the whole pipeline
# MAGIC Runs notebooks 01 → 08 in order with a shared `run_id`, so one execution = one traceable DQ run.
# MAGIC In production, create a **Databricks Job** with the same notebooks as dependent tasks (see README) and schedule it.
# MAGIC The assertion notebook is the last task: if it fails, the job fails and nobody should trust the dashboards.

# COMMAND ----------

dbutils.widgets.text("catalog", "olist_gov", "Catalog")
dbutils.widgets.text("hash_salt_scope", "", "Secret scope for hashing salt (optional)")
from datetime import datetime, timezone
run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
args = {"catalog": dbutils.widgets.get("catalog"), "run_id": run_id, "hash_salt_scope": dbutils.widgets.get("hash_salt_scope")}

STEPS = ["01_ingest_raw", "03_dq_rule_registry", "04_dq_engine", "05_analytics_model",
         "06_semantic_layer", "07_governance_metadata", "08_pipeline_assertions"]   # 02_profile_sources and 10_defect_injection_test are run on demand

# COMMAND ----------

for step in STEPS:
    print(f"▶ {step}")
    dbutils.notebook.run(f"./{step}", timeout_seconds=3600, arguments=args)
print(f"Done. run_id={run_id}")
