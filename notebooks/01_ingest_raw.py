# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest the Olist CSVs into the raw layer
# MAGIC **Before running:** upload the 9 Olist CSV files to the Unity Catalog volume `<catalog>.raw.landing`
# MAGIC (Catalog Explorer -> raw -> landing -> Upload to this volume).
# MAGIC
# MAGIC Design choices
# MAGIC * **Everything is read as STRING.** Raw must never fail or silently change data (e.g. a zip like `01046` must not become `1046`).
# MAGIC * Each row gets lineage metadata: `_row_id`, `_source_file`, `_ingested_at`, `_run_id`.
# MAGIC * A **schema contract check** fails fast if a source file lost/renamed a column we depend on.
# MAGIC * An **ingestion log** records row counts per table, so later notebooks can prove `raw = valid + quarantined`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import functions as F

log = []
for tbl, fname in FILES.items():
    path = f"{LANDING}/{fname}"
    df = (spark.read.format("csv")
          .option("header", True)
          .option("multiLine", True)      # review comments contain line breaks
          .option("quote", '"').option("escape", '"')
          .load(path))                     # no inferSchema -> all STRING

    missing = [c for c in STAGING[tbl] if c not in df.columns]
    assert not missing, f"Schema contract broken for {tbl}: missing columns {missing}"

    df = (df.withColumn("_row_id", F.monotonically_increasing_id())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(RUN_ID)))

    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("raw", tbl)))
    n = spark.table(T("raw", tbl)).count()
    log.append((RUN_ID, tbl, fname, n))
    print(f"raw.{tbl:22s} {n:>10,} rows")

spark.createDataFrame(log, "run_id string, table_name string, source_file string, row_count long") \
     .withColumn("ingested_at", F.current_timestamp()) \
     .write.mode("append").saveAsTable(T("governance", "ingestion_log"))

# COMMAND ----------

display(spark.table(T("governance", "ingestion_log")).where(F.col("run_id") == RUN_ID))
