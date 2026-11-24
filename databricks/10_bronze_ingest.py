# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: NYC TLC trip records
# MAGIC Downloads monthly Parquet files to the landing volume, writes them to Delta (one table per service, partitioned by month), and logs one manifest row per file. Re-running a month replaces only that month.

# COMMAND ----------

dbutils.widgets.text("service", "yellow")
dbutils.widgets.text("start_month", "2024-01")
dbutils.widgets.text("end_month", "2024-01")

service = dbutils.widgets.get("service").strip().lower()
start_month = dbutils.widgets.get("start_month").strip()
end_month = dbutils.widgets.get("end_month").strip()
assert service in ("yellow", "green"), f"unsupported service: {service}"

# COMMAND ----------

import os, time, urllib.request
from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = "nyc_mobility"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"
TARGET = f"{CATALOG}.bronze.{service}_trips"
MANIFEST = f"{CATALOG}.ops.ingest_manifest"

# Source files change types between years and the airport fee column changes case.
# Bronze lower-cases names and casts to these types so every month lands in one schema.
CANONICAL = {
    "vendorid": "int", "passenger_count": "double", "trip_distance": "double",
    "ratecodeid": "double", "pulocationid": "int", "dolocationid": "int",
    "payment_type": "long", "fare_amount": "double", "extra": "double",
    "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double",
    "congestion_surcharge": "double", "airport_fee": "double",
    "ehail_fee": "double", "trip_type": "double", "cbd_congestion_fee": "double",
}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST} (
  service STRING, period STRING, source_url STRING, landing_path STRING,
  file_bytes BIGINT, rows_source BIGINT, rows_written BIGINT,
  status STRING, error STRING, seconds DOUBLE, ingested_at TIMESTAMP)
""")

def months(start, end):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)

def log(rec):
    schema = spark.table(MANIFEST).schema
    rec = {**rec, "ingested_at": datetime.now(timezone.utc).replace(tzinfo=None)}
    row = tuple(rec.get(f.name) for f in schema.fields)
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(MANIFEST)

# COMMAND ----------

def ingest(period):
    t0 = time.time()
    fname = f"{service}_tripdata_{period}.parquet"
    url = f"{BASE_URL}/{fname}"
    folder = f"{LANDING}/{service}/{period[:4]}"
    path = f"{folder}/{fname}"
    rec = {"service": service, "period": period, "source_url": url, "landing_path": path,
           "file_bytes": None, "rows_source": None, "rows_written": None,
           "status": "FAILED", "error": None}
    try:
        os.makedirs(folder, exist_ok=True)
        urllib.request.urlretrieve(url, path)
        rec["file_bytes"] = os.path.getsize(path)

        raw = spark.read.parquet(path)
        rec["rows_source"] = raw.count()
        df = raw.toDF(*[c.lower() for c in raw.columns])
        for c in df.columns:
            if c.endswith("_datetime"):
                df = df.withColumn(c, F.col(c).cast("timestamp"))
            elif c in CANONICAL:
                df = df.withColumn(c, F.col(c).cast(CANONICAL[c]))
        df = (df.withColumn("_service", F.lit(service))
                .withColumn("_period", F.lit(period))
                .withColumn("_source_file", F.lit(fname))
                .withColumn("_ingested_at", F.current_timestamp()))

        writer = df.write.format("delta").option("mergeSchema", "true")
        if spark.catalog.tableExists(TARGET):
            writer.mode("overwrite").option("replaceWhere", f"_period = '{period}'").saveAsTable(TARGET)
        else:
            writer.mode("append").partitionBy("_period").saveAsTable(TARGET)

        rec["rows_written"] = spark.table(TARGET).where(F.col("_period") == period).count()
        rec["status"] = "OK" if rec["rows_written"] == rec["rows_source"] else "ROWCOUNT_MISMATCH"
    except Exception as e:
        rec["error"] = repr(e)[:1000]
    rec["seconds"] = round(time.time() - t0, 1)
    log(rec)
    return rec

# COMMAND ----------

results = [ingest(p) for p in months(start_month, end_month)]
display(spark.createDataFrame([(r["period"], r["status"], r["rows_source"], r["rows_written"], r["seconds"], r["error"]) for r in results],
                              "period string, status string, rows_source long, rows_written long, seconds double, error string"))

bad = [r for r in results if r["status"] != "OK"]
assert not bad, f"{len(bad)} month(s) failed: {[(r['period'], r['status']) for r in bad]}"


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