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

path = "Files/bronze/bicing/info/info_2025-10.csv"

raw = spark.read.option("header", True).csv(path)
raw.printSchema()
raw.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze_info = (
    raw
    .withColumn("source_file", F.lit("info_2025-10.csv"))
    .withColumn("loaded_at", F.current_timestamp())
)

bronze_info.write.mode("overwrite").format("delta").saveAsTable("bronze.station_info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                    AS row_count,
           COUNT(DISTINCT station_id)  AS stations,
           COUNT(DISTINCT last_updated) AS snapshots,
           MIN(last_updated)           AS first_snapshot,
           MAX(last_updated)           AS last_snapshot
    FROM bronze.station_info
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)            AS station_count,
           MIN(snapshots)      AS min_snapshots,
           MAX(snapshots)      AS max_snapshots
    FROM (
        SELECT station_id, COUNT(DISTINCT last_updated) AS snapshots
        FROM bronze.station_info
        GROUP BY station_id
    )
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                                         AS stations,
           SUM(CASE WHEN names > 1 THEN 1 ELSE 0 END)       AS changed_name,
           SUM(CASE WHEN coords > 1 THEN 1 ELSE 0 END)      AS changed_coordinates,
           SUM(CASE WHEN capacities > 1 THEN 1 ELSE 0 END)  AS changed_capacity,
           SUM(CASE WHEN configs > 1 THEN 1 ELSE 0 END)     AS changed_configuration
    FROM (
        SELECT station_id,
               COUNT(DISTINCT name)                  AS names,
               COUNT(DISTINCT concat(lat, lon))      AS coords,
               COUNT(DISTINCT capacity)              AS capacities,
               COUNT(DISTINCT physical_configuration) AS configs
        FROM bronze.station_info
        GROUP BY station_id
    )
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT station_id, name, lat, lon,
           MIN(last_updated) AS first_seen,
           MAX(last_updated) AS last_seen,
           COUNT(*)          AS snapshots
    FROM bronze.station_info
    WHERE station_id IN (
        SELECT station_id
        FROM bronze.station_info
        GROUP BY station_id
        HAVING COUNT(DISTINCT name) > 1
            OR COUNT(DISTINCT concat(lat, lon)) > 1
    )
    GROUP BY station_id, name, lat, lon
    ORDER BY station_id, first_seen
""").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT station_id,
           COUNT(DISTINCT capacity)           AS capacities,
           MIN(try_cast(capacity AS INT))     AS min_capacity,
           MAX(try_cast(capacity AS INT))     AS max_capacity
    FROM bronze.station_info
    GROUP BY station_id
    HAVING COUNT(DISTINCT capacity) > 1
    ORDER BY capacities DESC, station_id
""").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base = "Files/bronze/bicing/info/"
files = sorted(f.name for f in notebookutils.fs.ls(base))
print(len(files))
files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols = {n: spark.read.option("header", True).csv(base + n).columns for n in files}
ref = cols[files[0]]

for n in files:
    print(n, len(cols[n]), set(cols[n]) ^ set(ref))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

parts = []
for n in files:
    df = (spark.read.option("header", True).csv(base + n)
          .withColumn("source_file", F.lit(n)))
    parts.append(df)

bronze_info = parts[0]
for df in parts[1:]:
    bronze_info = bronze_info.unionByName(df, allowMissingColumns=True)

bronze_info = bronze_info.withColumn("loaded_at", F.current_timestamp())
print(len(bronze_info.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(bronze_info.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("bronze.station_info"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT source_file,
           COUNT(*)                     AS row_count,
           COUNT(DISTINCT last_updated) AS snapshots,
           SUM(CASE WHEN V1 IS NULL OR V1 = 'NA' THEN 0 ELSE 1 END) AS v1_filled
    FROM bronze.station_info
    GROUP BY source_file
    ORDER BY source_file
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze_info = spark.table("bronze.station_info")

bronze_info.select([
    F.sum(F.when(F.col(c) == "NA", 1).otherwise(0)).alias(c)
    for c in bronze_info.columns
]).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for c in ["physical_configuration", "is_charging_station", "is_valet_station",
          "x_ride_code_support", "nearby_distance", "ttl"]:
    bronze_info.groupBy(c).count().orderBy(F.desc("count")).show(10, truncate=False)

print(bronze_info.filter(F.col("short_name") != F.col("station_id")).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_info.filter(F.col("physical_configuration").isNull()).show(truncate=False, vertical=True)

(bronze_info
    .filter(F.col("short_name") != F.col("station_id"))
    .select("station_id", "short_name")
    .distinct()
    .show(20, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze_info = spark.table("bronze.station_info")

def na_to_null(c):
    return F.when(F.col(c) == "NA", None).otherwise(F.col(c))

typed = bronze_info.select(
    F.col("last_updated").cast("long").cast("timestamp").alias("snapshot_at"),
    F.col("station_id").cast("int").alias("station_id"),
    F.col("name"),
    F.col("physical_configuration"),
    F.col("lat").cast("double").alias("lat"),
    F.col("lon").cast("double").alias("lon"),
    na_to_null("altitude").cast("double").alias("altitude"),
    F.col("address"),
    F.col("cross_street"),
    F.lpad(na_to_null("post_code"), 5, "0").alias("post_code"),
    F.col("capacity").cast("int").alias("capacity"),
    (F.col("is_charging_station") == "TRUE").alias("is_charging_station"),
    F.col("short_name"),
)
typed.printSchema()
typed.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

located = (
    typed
    .withColumn("district_part", F.split("cross_street", "/").getItem(0))
    .withColumn("neighbourhood_part", F.split("cross_street", "/").getItem(1))
    .withColumn("district_code", F.regexp_extract("district_part", r"^(\d+)-", 1))
    .withColumn("district", F.regexp_replace("district_part", r"^\d+-", ""))
    .withColumn("neighbourhood_code", F.regexp_extract("neighbourhood_part", r"^(\d+)-", 1))
    .withColumn("neighbourhood", F.regexp_replace("neighbourhood_part", r"^\d+-", ""))
    .drop("cross_street", "district_part", "neighbourhood_part")
)

located.select("district_code", "district").distinct().orderBy("district_code").show(30, truncate=False)
print(located.select("neighbourhood_code").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(located.filter(F.col("district_code").isNull()).count())

dups = located.groupBy("station_id", "snapshot_at").count().filter("count > 1")
print(dups.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                                          AS duplicated_keys,
           SUM(CASE WHEN files > 1 THEN 1 ELSE 0 END)        AS across_files,
           SUM(CASE WHEN names > 1 OR caps > 1 THEN 1 ELSE 0 END) AS differing_values,
           MIN(n)                                            AS min_rows,
           MAX(n)                                            AS max_rows
    FROM (
        SELECT station_id, last_updated,
               COUNT(*)                    AS n,
               COUNT(DISTINCT source_file) AS files,
               COUNT(DISTINCT name)        AS names,
               COUNT(DISTINCT capacity)    AS caps
        FROM bronze.station_info
        WHERE last_updated RLIKE '^[0-9]+$'
        GROUP BY station_id, last_updated
        HAVING COUNT(*) > 1
    )
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(located
    .filter(F.col("district_code").isNull())
    .groupBy("physical_configuration")
    .agg(F.countDistinct("station_id").alias("stations"),
         F.count("*").alias("rows"))
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

located_clean = located.distinct()

print(located.count(), located_clean.count())
print(located_clean.groupBy("station_id", "snapshot_at").count().filter("count > 1").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(located_clean
    .filter(F.col("district_code").isNull())
    .groupBy("station_id", "name", "physical_configuration")
    .agg(F.count("*").alias("rows"),
         F.min("snapshot_at").alias("first_seen"),
         F.max("snapshot_at").alias("last_seen"))
    .orderBy("station_id")
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window

tracked = ["name", "physical_configuration", "lat", "lon", "altitude", "address",
           "district_code", "district", "neighbourhood_code", "neighbourhood",
           "post_code", "capacity", "is_charging_station", "short_name"]

w = Window.partitionBy("station_id").orderBy("snapshot_at")

versions = (
    located_clean
    .withColumn("attr_hash", F.xxhash64(*tracked))
    .withColumn("changed", F.when(F.col("attr_hash") != F.lag("attr_hash").over(w), 1).otherwise(0))
    .withColumn("version_no", F.sum("changed").over(w) + 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(versions
    .filter("station_id = 72")
    .groupBy("version_no", "name")
    .agg(F.min("snapshot_at").alias("first_seen"),
         F.max("snapshot_at").alias("last_seen"),
         F.count("*").alias("snapshots"))
    .orderBy("version_no")
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_info = (
    versions
    .groupBy("station_id", "version_no", *tracked)
    .agg(F.min("snapshot_at").alias("first_seen_at"),
         F.max("snapshot_at").alias("last_seen_at"),
         F.count("*").alias("snapshots"))
    .select("station_id", "version_no", "first_seen_at", "last_seen_at", "snapshots", *tracked)
)

(silver_info.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("silver.station_info"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                   AS versions,
           COUNT(DISTINCT station_id) AS stations,
           SUM(snapshots)             AS snapshots,
           MIN(first_seen_at)         AS first_seen,
           MAX(last_seen_at)          AS last_seen
    FROM silver.station_info
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w2 = Window.partitionBy("station_id").orderBy("version_no")

diff = silver_info
for c in tracked:
    diff = diff.withColumn("chg_" + c, (~F.col(c).eqNullSafe(F.lag(c).over(w2))).cast("int"))
diff = diff.filter("version_no > 1")

print(diff.count())
diff.select([F.sum("chg_" + c).alias(c) for c in tracked]).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_info = spark.table("silver.station_info")

w_station = Window.partitionBy("station_id")
w_last = w_station.orderBy(F.desc("version_no"))

dim_station = (
    silver_info
    .withColumn("first_seen_at", F.min("first_seen_at").over(w_station))
    .withColumn("last_seen_at", F.max("last_seen_at").over(w_station))
    .withColumn("rn", F.row_number().over(w_last))
    .filter("rn = 1")
    .withColumn("station_kind",
                F.when(F.col("name").rlike("(?i)testing|pruebas"), "Test")
                 .when(F.col("physical_configuration") == "VALET", "Event")
                 .otherwise("Regular"))
    .withColumn("district", F.coalesce("district", F.lit("Unassigned")))
    .withColumn("neighbourhood", F.coalesce("neighbourhood", F.lit("Unassigned")))
    .select("station_id", "name", "station_kind", "physical_configuration",
            "address", "lat", "lon", "altitude",
            "district_code", "district", "neighbourhood_code", "neighbourhood",
            "post_code", "is_charging_station", "first_seen_at", "last_seen_at")
)

print(dim_station.count())
dim_station.groupBy("station_kind").count().show()
dim_station.filter("station_kind != 'Regular'").select("station_id", "name", "station_kind").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(dim_station.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("gold.dim_station"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT district, COUNT(*) AS stations
    FROM gold.dim_station
    WHERE station_kind = 'Regular'
    GROUP BY district
    ORDER BY stations DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

silver_info = spark.table("silver.station_info")
w_cap = Window.partitionBy("station_id").orderBy("version_no")

runs = (
    silver_info
    .withColumn("cap_changed", F.when(F.col("capacity") != F.lag("capacity").over(w_cap), 1).otherwise(0))
    .withColumn("cap_run", F.sum("cap_changed").over(w_cap) + 1)
    .groupBy("station_id", "cap_run", "capacity")
    .agg(F.min("first_seen_at").alias("first_seen_at"))
)

w_run = Window.partitionBy("station_id").orderBy("cap_run")

station_capacity = (
    runs
    .withColumn("run_start", F.date_trunc("hour", F.from_utc_timestamp("first_seen_at", "Europe/Madrid")))
    .withColumn("valid_from",
                F.when(F.col("cap_run") == 1, F.lit("1900-01-01 00:00:00").cast("timestamp"))
                 .otherwise(F.col("run_start")))
    .withColumn("valid_to",
                F.coalesce(F.lead("run_start").over(w_run), F.lit("9999-12-31 00:00:00").cast("timestamp")))
    .select("station_id", "capacity", "valid_from", "valid_to")
)

(station_capacity.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("gold.station_capacity"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                   AS ranges,
           COUNT(DISTINCT station_id) AS stations,
           SUM(CASE WHEN valid_from = TIMESTAMP '1900-01-01 00:00:00' THEN 1 ELSE 0 END) AS first_ranges,
           SUM(CASE WHEN valid_to   = TIMESTAMP '9999-12-31 00:00:00' THEN 1 ELSE 0 END) AS open_ranges
    FROM gold.station_capacity
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

f = spark.table("gold.fact_station_availability").alias("f")
c = spark.table("gold.station_capacity").alias("c")

joined = f.join(
    c,
    (F.col("f.station_id") == F.col("c.station_id"))
    & (F.col("f.hour_local") >= F.col("c.valid_from"))
    & (F.col("c.valid_to").isNull() | (F.col("f.hour_local") < F.col("c.valid_to"))),
    "left",
)

print(f.count(), joined.count())
print(joined.filter(F.col("c.capacity").isNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing = (joined
    .filter(F.col("c.capacity").isNull())
    .select(F.col("f.station_id").alias("station_id"),
            F.col("f.hour_local").alias("hour_local")))

d = spark.table("gold.dim_station").select("station_id", "station_kind")

(missing.join(d, "station_id", "left")
    .groupBy("station_kind")
    .agg(F.countDistinct("station_id").alias("stations"),
         F.count("*").alias("rows"),
         F.min("hour_local").alias("first_hour"),
         F.max("hour_local").alias("last_hour"))
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(joined
    .withColumn("period",
                F.when(F.col("f.hour_local") < F.lit("2024-11-01"), "before window")
                 .when(F.col("f.hour_local") >= F.lit("2025-11-01"), "after window")
                 .otherwise("window"))
    .groupBy("period")
    .agg(F.count("*").alias("rows"),
         F.sum(F.when(F.col("c.capacity").isNull(), 1).otherwise(0)).alias("no_capacity"),
         F.min("f.hour_local").alias("first_hour"),
         F.max("f.hour_local").alias("last_hour"))
    .orderBy("period")
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a = spark.table("gold.dim_station")
b = (spark.table("gold.dim_station_dfg2")
     .withColumnRenamed("station_first_seen_at", "first_seen_at")
     .withColumnRenamed("station_last_seen_at", "last_seen_at"))

print(a.count(), b.count())

da, db = dict(a.dtypes), dict(b.dtypes)
for c in a.columns:
    print(f"{c:25} {da.get(c)!s:12} {db.get(c)!s:12} {'' if da.get(c) == db.get(c) else '<-- differs'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols = a.columns
only_notebook = a.select(cols).exceptAll(b.select(cols))
only_dataflow = b.select(cols).exceptAll(a.select(cols))

print(only_notebook.count(), only_dataflow.count())
only_notebook.show(10, truncate=False)
only_dataflow.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
