# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: vendor dimension with SCD Type 2 history
# MAGIC Applies each new SQL Server vendor snapshot in time order. A changed attribute closes the current row (valid_to) and inserts a new version; a vendor missing from a snapshot is closed; unchanged vendors are untouched. A watermark makes reruns process only new snapshots.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "nyc_mobility"
SOURCE = f"{CATALOG}.bronze.sqlserver_vendor"
TARGET = f"{CATALOG}.silver.dim_vendor_scd2"
WM = f"{CATALOG}.ops.watermarks"
PIPELINE = "silver.dim_vendor_scd2"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  vendor_sk STRING, vendor_id INT, vendor_name STRING, platform_fee_pct DECIMAL(5,2), is_active BOOLEAN,
  row_hash STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)
""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {WM} (pipeline STRING, last_value TIMESTAMP, updated_at TIMESTAMP)")

last = spark.sql(f"SELECT max(last_value) AS v FROM {WM} WHERE pipeline = '{PIPELINE}'").first()["v"]
snaps = spark.table(SOURCE).select(F.col("extracted_at").cast("timestamp").alias("ts")).distinct()
if last is not None:
    snaps = snaps.where(F.col("ts") > F.lit(last))
todo = sorted(r["ts"] for r in snaps.collect())
print(f"{len(todo)} new snapshot(s)")

# COMMAND ----------

for ts in todo:
    snap = (spark.table(SOURCE)
            .where(F.col("extracted_at").cast("timestamp") == F.lit(ts))
            .select(F.col("vendor_id").cast("int").alias("vendor_id"),
                    "vendor_name",
                    F.col("platform_fee_pct").cast("decimal(5,2)").alias("platform_fee_pct"),
                    F.col("is_active").cast("boolean").alias("is_active"),
                    F.col("extracted_at").cast("timestamp").alias("extracted_at"))
            .withColumn("row_hash", F.sha2(F.concat_ws("||", "vendor_name",
                        F.col("platform_fee_pct").cast("string"), F.col("is_active").cast("string")), 256)))
    snap.createOrReplaceTempView("snap")
    ts_sql = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(f"""
      MERGE INTO {TARGET} t
      USING (
        SELECT vendor_id AS merge_key, * FROM snap
        UNION ALL
        SELECT NULL AS merge_key, s.* FROM snap s
        JOIN {TARGET} c ON s.vendor_id = c.vendor_id AND c.is_current AND c.row_hash <> s.row_hash
      ) u
      ON t.vendor_id = u.merge_key AND t.is_current
      WHEN MATCHED AND t.row_hash <> u.row_hash THEN
        UPDATE SET is_current = false, valid_to = u.extracted_at
      WHEN NOT MATCHED THEN
        INSERT (vendor_sk, vendor_id, vendor_name, platform_fee_pct, is_active, row_hash, valid_from, valid_to, is_current)
        VALUES (sha2(concat(u.vendor_id, '|', cast(u.extracted_at AS STRING)), 256), u.vendor_id, u.vendor_name,
                u.platform_fee_pct, u.is_active, u.row_hash, u.extracted_at, NULL, true)
      WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
        UPDATE SET is_current = false, valid_to = TIMESTAMP'{ts_sql}'
    """)
    spark.sql(f"INSERT INTO {WM} VALUES ('{PIPELINE}', TIMESTAMP'{ts_sql}', current_timestamp())")
    print(f"applied snapshot {ts}")

# COMMAND ----------

display(spark.sql(f"""
  SELECT vendor_id, vendor_name, platform_fee_pct, valid_from, valid_to, is_current
  FROM {TARGET} ORDER BY vendor_id, valid_from
"""))