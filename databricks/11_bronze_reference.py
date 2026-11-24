# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze reference: TLC taxi zone lookup
# MAGIC Loads the official zone lookup (LocationID to borough, zone, service zone) as a Delta table. Full reload each run.

# COMMAND ----------

import os, urllib.request
from pyspark.sql import functions as F

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
PATH = "/Volumes/nyc_mobility/bronze/landing/reference/taxi_zone_lookup.csv"
TARGET = "nyc_mobility.bronze.taxi_zones"

os.makedirs(os.path.dirname(PATH), exist_ok=True)
urllib.request.urlretrieve(URL, PATH)

df = (spark.read.option("header", True).csv(PATH)
        .toDF("location_id", "borough", "zone", "service_zone")
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("_source_file", F.lit("taxi_zone_lookup.csv"))
        .withColumn("_ingested_at", F.current_timestamp()))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

display(spark.sql(f"SELECT COUNT(*) AS zones, COUNT(DISTINCT location_id) AS ids, MIN(location_id) AS min_id, MAX(location_id) AS max_id FROM {TARGET}"))