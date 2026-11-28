# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: SAP SD extract (full reload)
# MAGIC Reads the semicolon-delimited SAP extract files as delivered (all columns as strings, SAP field names kept) and replaces the Bronze tables. The source sends full extracts, so Bronze is reloaded in full; conformance happens in dbt staging.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "nyc_mobility"
BASE = f"/Volumes/{CATALOG}/bronze/landing/sap"

for table in ["KNA1", "MARA", "MAKT", "VBRK", "VBRP"]:
    df = (spark.read.option("header", True).option("delimiter", ";").option("inferSchema", False)
          .csv(f"{BASE}/{table}")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.bronze.sap_{table.lower()}")

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'sap_kna1' AS table_name, COUNT(*) AS row_count FROM {CATALOG}.bronze.sap_kna1
  UNION ALL SELECT 'sap_mara', COUNT(*) FROM {CATALOG}.bronze.sap_mara
  UNION ALL SELECT 'sap_makt', COUNT(*) FROM {CATALOG}.bronze.sap_makt
  UNION ALL SELECT 'sap_vbrk', COUNT(*) FROM {CATALOG}.bronze.sap_vbrk
  UNION ALL SELECT 'sap_vbrp', COUNT(*) FROM {CATALOG}.bronze.sap_vbrp
"""))