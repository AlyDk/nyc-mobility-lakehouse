# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze ingestion: NYC TLC trip records
# MAGIC Downloads monthly Parquet files to the landing volume, writes them to Delta (one table per service, partitioned by month), and logs one manifest row per file. Re-running a month replaces only that month.

# COMMAND ----------

display(spark.sql("""
  SELECT service, COUNT(*) AS months, SUM(IF(status = 'OK', 1, 0)) AS ok_months, SUM(rows_written) AS rows
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service, period ORDER BY ingested_at DESC) AS rn
    FROM nyc_mobility.ops.ingest_manifest
  )
  WHERE rn = 1
  GROUP BY service
  ORDER BY service
"""))