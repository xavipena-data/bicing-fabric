# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "77fdc15c-c9d6-4cdc-b0e1-daa5fbf08ea6",
# META       "default_lakehouse_name": "lh_bicing",
# META       "default_lakehouse_workspace_id": "a49f3c43-0b7b-4eeb-8ee3-83e6d027f990",
# META       "known_lakehouses": [
# META         {
# META           "id": "77fdc15c-c9d6-4cdc-b0e1-daa5fbf08ea6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

bronze = spark.table("bronze.station_status")

for c in ["is_installed", "is_renting", "is_returning", "is_charging_station", "status"]:
    print(c)
    bronze.groupBy(c).count().orderBy("count", ascending=False).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bad = bronze.filter(F.col("status") == "NA")
bad.groupBy("source_file").count().show(truncate=False)
bad.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_cols = ["station_id", "num_bikes_available",
            "num_bikes_available_types.mechanical", "num_bikes_available_types.ebike",
            "num_docks_available", "last_reported", "last_updated", "ttl"]

valid = bronze.filter(F.col("station_id") != "NA")

exprs = [F.sum(F.expr(f"CASE WHEN try_cast(`{c}` AS BIGINT) IS NULL THEN 1 ELSE 0 END")).alias(c.replace(".", "_"))
         for c in num_cols]

valid.agg(*exprs).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

odd = valid.filter(F.expr("try_cast(last_reported AS BIGINT) IS NULL"))
odd.groupBy("source_file", "last_reported").count().orderBy(F.desc("count")).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid.select("last_updated", "last_reported").show(3, truncate=False)

lag = (valid.filter(F.expr("try_cast(last_reported AS BIGINT) IS NOT NULL"))
       .select((F.col("last_updated").cast("long") - F.col("last_reported").cast("long")).alias("lag_s")))

lag.agg(F.min("lag_s"), F.expr("percentile_approx(lag_s, 0.5)"),
        F.expr("percentile_approx(lag_s, 0.99)"), F.max("lag_s")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_bool(c):
    return (F.when(F.col(c).isin("1", "TRUE"), True)
             .when(F.col(c).isin("0", "FALSE"), False))

silver = valid.select(
    F.col("last_updated").cast("long").cast("timestamp").alias("snapshot_at"),
    F.col("station_id").cast("int").alias("station_id"),
    F.col("num_bikes_available").cast("int").alias("bikes_available"),
    F.col("`num_bikes_available_types.mechanical`").cast("int").alias("mechanical_bikes"),
    F.col("`num_bikes_available_types.ebike`").cast("int").alias("ebikes"),
    F.col("num_docks_available").cast("int").alias("docks_available"),
    F.expr("try_cast(last_reported AS BIGINT)").cast("timestamp").alias("last_reported_at"),
    to_bool("is_installed").alias("is_installed"),
    to_bool("is_renting").alias("is_renting"),
    to_bool("is_returning").alias("is_returning"),
    to_bool("is_charging_station").alias("is_charging_station"),
    F.col("status"),
    F.col("ttl").cast("int").alias("ttl"),
    F.col("source_file"),
)

silver.printSchema()
silver.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

checks = silver.agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("bikes_available") != F.col("mechanical_bikes") + F.col("ebikes"), 1).otherwise(0)).alias("bike_sum_mismatch"),
    F.sum(F.when((F.col("bikes_available") < 0) | (F.col("docks_available") < 0), 1).otherwise(0)).alias("negatives"),
    F.sum(F.when(F.col("last_reported_at").isNull(), 1).otherwise(0)).alias("null_last_reported"),
)
checks.show()

print("distinct (station, snapshot):", silver.select("station_id", "snapshot_at").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8) Duplicates: how many times a key repeats, and across how many files
dups = (silver.groupBy("station_id", "snapshot_at")
        .agg(F.count("*").alias("n"), F.countDistinct("source_file").alias("files"))
        .filter("n > 1"))
dups.groupBy("n", "files").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9) Mismatches: which files, and a few examples
mm = silver.filter(F.col("bikes_available") != F.col("mechanical_bikes") + F.col("ebikes"))
mm.groupBy("source_file").count().orderBy("source_file").show(20, truncate=False)
mm.select("snapshot_at", "station_id", "bikes_available", "mechanical_bikes", "ebikes", "status").show(8, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 10) Do mismatches happen only when the station can't rent bikes?
mm.groupBy("status", "is_renting").count().orderBy(F.desc("count")).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11) Are duplicates exact copies?
dup_rows = silver.join(dups.select("station_id", "snapshot_at"), ["station_id", "snapshot_at"])
print("rows in duplicated keys:", dup_rows.count())
print("distinct rows ignoring source_file:", dup_rows.drop("source_file").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window

w = Window.partitionBy("station_id", "snapshot_at").orderBy("source_file")

silver_clean = (silver
                .withColumn("rn", F.row_number().over(w))
                .filter("rn = 1")
                .drop("rn"))

print(silver_clean.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_clean.write.mode("overwrite").format("delta").saveAsTable("silver.station_status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                AS row_count,
           COUNT(DISTINCT station_id) AS stations,
           MIN(snapshot_at)        AS first_snapshot,
           MAX(snapshot_at)        AS last_snapshot
    FROM silver.station_status
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
