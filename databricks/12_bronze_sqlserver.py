# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: SQL Server master data snapshots (Auto Loader)
# MAGIC Picks up new Parquet snapshots landed by scripts/extract_sqlserver.py and appends them to Bronze. A checkpoint makes sure each file is read exactly once.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "nyc_mobility"
BASE = f"/Volumes/{CATALOG}/bronze/landing/sqlserver"
CHECKPOINTS = f"/Volumes/{CATALOG}/bronze/landing/_checkpoints/sqlserver"

for name in ["vendor", "rate_code"]:
    query = (spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "parquet")
             .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{name}/schema")
             .load(f"{BASE}/{name}")
             .withColumn("_source_file", F.col("_metadata.file_path"))
             .withColumn("_ingested_at", F.current_timestamp())
             .writeStream
             .option("checkpointLocation", f"{CHECKPOINTS}/{name}/checkpoint")
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(f"{CATALOG}.bronze.sqlserver_{name}"))
    query.awaitTermination()
    print(f"{name}: done")

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'vendor' AS source, COUNT(*) AS rows, COUNT(DISTINCT _source_file) AS snapshots FROM {CATALOG}.bronze.sqlserver_vendor
  UNION ALL
  SELECT 'rate_code', COUNT(*), COUNT(DISTINCT _source_file) FROM {CATALOG}.bronze.sqlserver_rate_code
"""))