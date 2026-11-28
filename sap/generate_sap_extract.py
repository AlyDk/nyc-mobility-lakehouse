# Databricks notebook source
# MAGIC %md
# MAGIC # SAP SD extract simulator (SYNTHETIC DATA)
# MAGIC Simulates the extract an SAP ECC / S/4HANA system would deliver for corporate taxi accounts: customer master (KNA1), material master (MARA, MAKT), billing documents (VBRK header, VBRP items).
# MAGIC
# MAGIC **All SAP data here is synthetic.** Invoices are derived from real trips with a deterministic rule: credit-card and Flex Fare trips picked up in Manhattan whose trip_id hash falls in a 5% bucket are billed to one of 40 fictional corporate customers.
# MAGIC Field names and formats follow SAP conventions: client MANDT, zero-padded keys, dates as YYYYMMDD, semicolon-delimited CSV per table, billing split by month.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "nyc_mobility"
OUT = f"/Volumes/{CATALOG}/bronze/landing/sap"
MANDT = "100"
N_CUSTOMERS = 40

PREFIX = ["Hudson", "Empire", "Liberty", "Chelsea", "Battery", "Gotham", "Madison", "Lexington", "Soho", "Tribeca"]
SUFFIX = ["Legal Partners", "Capital Advisors", "Media Group", "Health Systems"]
NAMES = [f"{p} {s}" for s in SUFFIX for p in PREFIX]
MATERIALS = [("TX-STD", "Taxi trip, standard"), ("TX-AIRPORT", "Taxi trip, airport"), ("TX-FLEX", "Taxi trip, flex fare")]

kna1 = spark.createDataFrame(
    [(MANDT, f"{100001 + i:010d}", NAMES[i], "New York", "US", "Z001") for i in range(N_CUSTOMERS)],
    "MANDT string, KUNNR string, NAME1 string, ORT01 string, LAND1 string, KTOKD string")
mara = spark.createDataFrame([(MANDT, m, "DIEN", "TAXI", "EA") for m, _ in MATERIALS],
                             "MANDT string, MATNR string, MTART string, MATKL string, MEINS string")
makt = spark.createDataFrame([(MANDT, m, "E", t) for m, t in MATERIALS],
                             "MANDT string, MATNR string, SPRAS string, MAKTX string")

# COMMAND ----------

trips = (spark.table(f"{CATALOG}.gold.fct_trips").alias("f")
         .join(spark.table(f"{CATALOG}.gold.dim_zone").alias("z"), F.col("f.pu_zone_key") == F.col("z.zone_key"))
         .where((F.col("z.borough") == "Manhattan") & F.col("f.payment_type_key").isin(0, 1))
         .withColumn("h", F.abs(F.xxhash64("f.trip_id")))
         .where(F.col("h") % 20 == 0)
         .select((F.abs(F.xxhash64(F.col("f.trip_id"), F.lit("customer"))) % N_CUSTOMERS).alias("cust_idx"),
                 F.substring(F.col("f.pickup_date_key").cast("string"), 1, 6).alias("yyyymm"),
                 F.when(F.col("f.is_flex_fare"), "TX-FLEX")
                  .when(F.col("f.is_airport_trip"), "TX-AIRPORT")
                  .otherwise("TX-STD").alias("MATNR"),
                 F.col("f.total_amount")))

items = trips.groupBy("cust_idx", "yyyymm", "MATNR").agg(F.count("*").alias("FKIMG"),
                                                           F.sum("total_amount").alias("NETWR"))

hdr = (items.select("cust_idx", "yyyymm").distinct()
       .withColumn("VBELN", F.lpad((F.row_number().over(Window.orderBy("yyyymm", "cust_idx")) + 90000000).cast("string"), 10, "0"))
       .withColumn("KUNAG", F.lpad((F.col("cust_idx") + 100001).cast("string"), 10, "0"))
       .withColumn("FKDAT", F.date_format(F.last_day(F.to_date(F.concat("yyyymm", F.lit("01")), "yyyyMMdd")), "yyyyMMdd")))

vbrp = (items.join(hdr, ["cust_idx", "yyyymm"])
        .withColumn("POSNR", F.lpad((F.row_number().over(Window.partitionBy("VBELN").orderBy("MATNR")) * 10).cast("string"), 6, "0"))
        .select(F.lit(MANDT).alias("MANDT"), "VBELN", "POSNR", "MATNR", "FKIMG", F.lit("EA").alias("VRKME"),
                F.col("NETWR").cast("decimal(15,2)").alias("NETWR"), "yyyymm"))

vbrk = (vbrp.groupBy("VBELN", "yyyymm").agg(F.sum("NETWR").alias("NETWR"))
        .join(hdr.drop("yyyymm"), "VBELN")
        .select(F.lit(MANDT).alias("MANDT"), "VBELN", F.lit("F2").alias("FKART"), "FKDAT", "KUNAG",
                F.lit("1000").alias("BUKRS"), F.lit("USD").alias("WAERK"),
                F.col("NETWR").cast("decimal(15,2)").alias("NETWR"), "yyyymm"))

# COMMAND ----------

def write_csv(df, name, partition=None):
    writer = df.coalesce(1).write.mode("overwrite").option("header", True).option("delimiter", ";")
    if partition:
        writer = writer.partitionBy(partition)
    writer.csv(f"{OUT}/{name}")

write_csv(kna1, "KNA1")
write_csv(mara, "MARA")
write_csv(makt, "MAKT")
write_csv(vbrk, "VBRK", "yyyymm")
write_csv(vbrp, "VBRP", "yyyymm")

# COMMAND ----------

display(vbrk.agg(F.countDistinct("KUNAG").alias("customers"), F.count("*").alias("invoices"),
                 F.sum("NETWR").alias("billed_usd"))
        .crossJoin(vbrp.agg(F.count("*").alias("invoice_items"), F.sum("FKIMG").alias("billed_trips"))))