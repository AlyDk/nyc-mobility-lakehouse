# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: conformed trips
# MAGIC Unifies yellow and green Bronze into one trips table: standard names, deterministic trip_id, data-quality flags, is_valid. Invalid rows are flagged, not dropped, so Silver always reconciles to Bronze.
# MAGIC Incremental: only (service, month) pairs whose Bronze load is newer than Silver are processed, using MERGE (insert new, update changed, delete rows that vanished from a reloaded month).

# COMMAND ----------

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
mode = dbutils.widgets.get("mode")

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "nyc_mobility"
TARGET = f"{CATALOG}.silver.trips"
SERVICES = {
    "yellow": {"table": f"{CATALOG}.bronze.yellow_trips", "pickup": "tpep_pickup_datetime", "dropoff": "tpep_dropoff_datetime"},
    "green":  {"table": f"{CATALOG}.bronze.green_trips",  "pickup": "lpep_pickup_datetime", "dropoff": "lpep_dropoff_datetime"},
}
PAYMENT = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}
KEY_COLS = ["service", "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "trip_distance_mi", "total_amount"]
BLOCKING = ["negative_amount", "zero_distance", "distance_over_200mi", "dropoff_not_after_pickup", "duration_over_24h", "pickup_outside_period"]

def conform(service, cfg, periods):
    b = spark.table(cfg["table"]).where(F.col("_period").isin(periods))
    def opt(name, t="double"):
        return (F.col(name) if name in b.columns else F.lit(None)).cast(t).alias(name)
    df = b.select(
        F.lit(service).alias("service"),
        F.col("vendorid").alias("vendor_id"),
        F.col(cfg["pickup"]).alias("pickup_ts"),
        F.col(cfg["dropoff"]).alias("dropoff_ts"),
        F.col("pulocationid").alias("pu_location_id"),
        F.col("dolocationid").alias("do_location_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").alias("trip_distance_mi"),
        F.col("ratecodeid").cast("int").alias("rate_code_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        opt("congestion_surcharge"), opt("airport_fee"), opt("cbd_congestion_fee"), opt("trip_type", "int"),
        "total_amount", "_period", "_source_file", "_ingested_at",
    )
    pay_map = F.create_map(*[F.lit(x) for kv in PAYMENT.items() for x in kv])
    df = (df
        .withColumn("duration_min", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0)
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("payment_desc", F.coalesce(pay_map[F.col("payment_type")], F.lit("Missing")))
        .withColumn("is_flex_fare", F.coalesce(F.col("payment_type") == 0, F.lit(False)))
        .withColumn("trip_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in KEY_COLS]), 256)))
    rules = {
        "negative_amount": (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
        "zero_distance": F.col("trip_distance_mi") <= 0,
        "distance_over_200mi": F.col("trip_distance_mi") > 200,
        "dropoff_not_after_pickup": F.col("dropoff_ts") <= F.col("pickup_ts"),
        "duration_over_24h": F.col("duration_min") > 1440,
        "pickup_outside_period": F.date_format("pickup_ts", "yyyy-MM") != F.col("_period"),
        "passenger_zero": F.col("passenger_count") == 0,
        "unknown_zone": F.col("pu_location_id").isin(264, 265) | F.col("do_location_id").isin(264, 265),
    }
    df = df.withColumn("dq_flags", F.array_compact(F.array(*[F.when(c, F.lit(n)) for n, c in rules.items()])))
    df = df.withColumn("is_valid", F.size(F.array_intersect("dq_flags", F.array(*[F.lit(x) for x in BLOCKING]))) == 0)
    w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return df.withColumn("_silver_updated_at", F.current_timestamp())

def periods_to_process(service, cfg):
    b = spark.table(cfg["table"]).groupBy("_period").agg(F.max("_ingested_at").alias("b_ts"))
    if mode == "full" or not spark.catalog.tableExists(TARGET):
        return sorted(r["_period"] for r in b.collect())
    s = spark.table(TARGET).where(F.col("service") == service).groupBy("_period").agg(F.max("_ingested_at").alias("s_ts"))
    todo = b.join(s, "_period", "left").where(F.col("s_ts").isNull() | (F.col("b_ts") > F.col("s_ts")))
    return sorted(r["_period"] for r in todo.collect())

# COMMAND ----------

for service, cfg in SERVICES.items():
    periods = periods_to_process(service, cfg)
    if not periods:
        print(f"{service}: nothing new")
        continue
    print(f"{service}: processing {len(periods)} month(s) {periods[0]}..{periods[-1]}")
    src = conform(service, cfg, periods)
    if not spark.catalog.tableExists(TARGET):
        src.write.format("delta").partitionBy("_period").saveAsTable(TARGET)
        print(f"{service}: created {TARGET}")
        continue
    src.createOrReplaceTempView("src")
    plist = ",".join(f"'{p}'" for p in periods)
    display(spark.sql(f"""
        MERGE INTO {TARGET} t
        USING src s
        ON t.trip_id = s.trip_id AND t._period = s._period
        WHEN MATCHED AND s._ingested_at > t._ingested_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.service = '{service}' AND t._period IN ({plist}) THEN DELETE
    """))

# COMMAND ----------

display(spark.sql(f"""
  SELECT s.service, b.bronze_rows, s.silver_rows, b.bronze_rows - s.silver_rows AS duplicates_removed,
         s.valid_rows, ROUND(100 * s.valid_rows / s.silver_rows, 2) AS valid_pct, s.flex_fare_rows
  FROM (SELECT service, COUNT(*) AS silver_rows, SUM(IF(is_valid, 1, 0)) AS valid_rows,
               SUM(IF(is_flex_fare, 1, 0)) AS flex_fare_rows
        FROM {TARGET} GROUP BY service) s
  JOIN (SELECT 'yellow' AS service, COUNT(*) AS bronze_rows FROM {CATALOG}.bronze.yellow_trips
        UNION ALL SELECT 'green', COUNT(*) FROM {CATALOG}.bronze.green_trips) b
  USING (service)
  ORDER BY s.service
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT service, flag, COUNT(*) AS rows
  FROM (SELECT service, explode(dq_flags) AS flag FROM {TARGET})
  GROUP BY service, flag
  ORDER BY service, flag
"""))