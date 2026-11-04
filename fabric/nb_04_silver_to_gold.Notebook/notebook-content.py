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

from pyspark.sql import functions as F

silver = spark.table("silver.station_status")

staged = silver.withColumn(
    "hour_local",
    F.date_trunc("hour", F.from_utc_timestamp("snapshot_at", "Europe/Madrid"))
)

staged.select("snapshot_at", "hour_local").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (staged.groupBy("station_id", "hour_local").agg(
    F.count("*").alias("readings"),
    F.round(F.avg("bikes_available"), 2).alias("avg_bikes_available"),
    F.min("bikes_available").alias("min_bikes_available"),
    F.max("bikes_available").alias("max_bikes_available"),
    F.round(F.avg("docks_available"), 2).alias("avg_docks_available"),
    F.sum(F.when(F.col("is_renting") & (F.col("bikes_available") == 0), 1).otherwise(0)).cast("int").alias("empty_readings"),
    F.sum(F.when(F.col("is_returning") & (F.col("docks_available") == 0), 1).otherwise(0)).cast("int").alias("full_readings"),
    F.sum(F.when(~F.col("is_renting"), 1).otherwise(0)).cast("int").alias("out_of_service_readings"),
))

fact.show(5, truncate=False)
print(fact.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cap = spark.table("gold.station_capacity").alias("c")
stations = spark.table("gold.dim_station").select("station_id")

window_fact = (fact
    .filter((F.col("hour_local") >= F.lit("2024-11-01")) &
            (F.col("hour_local") < F.lit("2025-11-01")))
    .join(stations, "station_id", "left_semi")
    .alias("f"))

fact_cap = (
    window_fact.join(
        cap,
        (F.col("f.station_id") == F.col("c.station_id"))
        & (F.col("f.hour_local") >= F.col("c.valid_from"))
        & (F.col("f.hour_local") < F.col("c.valid_to")),
        "left",
    )
    .select("f.*", F.col("c.capacity").alias("capacity"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(fact_cap.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("gold.fact_station_availability"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*)                                            AS row_count,
           SUM(readings)                                       AS readings,
           SUM(CASE WHEN capacity IS NULL THEN 1 ELSE 0 END)   AS no_capacity,
           MIN(hour_local)                                     AS first_hour,
           MAX(hour_local)                                     AS last_hour
    FROM gold.fact_station_availability
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT f.station_id, d.name, d.station_kind,
           COUNT(*)           AS rows,
           MIN(f.hour_local)  AS first_hour,
           MAX(f.hour_local)  AS last_hour,
           MIN(d.first_seen_at) AS info_first_seen
    FROM gold.fact_station_availability f
    LEFT JOIN gold.dim_station d ON f.station_id = d.station_id
    WHERE f.capacity IS NULL
    GROUP BY f.station_id, d.name, d.station_kind
    ORDER BY rows DESC
""").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT readings, COUNT(*) AS station_hours
    FROM gold.fact_station_availability
    GROUP BY readings
    ORDER BY readings
""").show(40)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for start, end in [("2025-10-26 00:00:00", "2025-10-26 04:00:00"),
                   ("2025-03-30 00:00:00", "2025-03-30 04:00:00")]:
    spark.sql(f"""
        SELECT hour_local,
               COUNT(*)      AS stations,
               MIN(readings) AS min_readings,
               MAX(readings) AS max_readings
        FROM gold.fact_station_availability
        WHERE hour_local BETWEEN '{start}' AND '{end}'
        GROUP BY hour_local
        ORDER BY hour_local
    """).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT date_trunc('hour', snapshot_at) AS hour_utc,
           COUNT(*)                        AS readings,
           COUNT(DISTINCT station_id)      AS stations
    FROM silver.station_status
    WHERE snapshot_at >= '2025-10-25 21:00:00'
      AND snapshot_at <  '2025-10-26 04:00:00'
    GROUP BY 1
    ORDER BY 1
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print(spark.conf.get("spark.sql.session.timeZone"))

hours = spark.sql("""
    SELECT explode(sequence(to_timestamp('2024-11-01 00:00:00'),
                            to_timestamp('2025-10-31 23:00:00'),
                            interval 1 hour)) AS hour_local
""")

dim_time = (
    hours
    .withColumn("date", F.to_date("hour_local"))
    .withColumn("year", F.year("hour_local"))
    .withColumn("month", F.month("hour_local"))
    .withColumn("month_name", F.date_format("hour_local", "MMMM"))
    .withColumn("year_month", F.date_format("hour_local", "yyyy-MM"))
    .withColumn("day_of_month", F.dayofmonth("hour_local"))
    .withColumn("day_of_week", ((F.dayofweek("hour_local") + 5) % 7) + 1)
    .withColumn("day_name", F.date_format("hour_local", "EEEE"))
    .withColumn("is_weekend", F.col("day_of_week") >= 6)
    .withColumn("hour_of_day", F.hour("hour_local"))
    .withColumn("day_part",
                F.when(F.col("hour_of_day") < 6, "Night")
                 .when(F.col("hour_of_day") < 12, "Morning")
                 .when(F.col("hour_of_day") < 18, "Afternoon")
                 .otherwise("Evening"))
    .withColumn("season",
                F.when(F.col("month").isin(12, 1, 2), "Winter")
                 .when(F.col("month").isin(3, 4, 5), "Spring")
                 .when(F.col("month").isin(6, 7, 8), "Summer")
                 .otherwise("Autumn"))
)

print(dim_time.count())
dim_time.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(dim_time.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("gold.dim_time"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT COUNT(*) AS fact_rows_without_time
    FROM gold.fact_station_availability f
    LEFT ANTI JOIN gold.dim_time t ON f.hour_local = t.hour_local
""").show()

spark.sql("""
    SELECT COUNT(*)           AS hours_without_facts,
           MIN(t.hour_local)  AS first_hour,
           MAX(t.hour_local)  AS last_hour
    FROM gold.dim_time t
    LEFT ANTI JOIN (SELECT DISTINCT hour_local FROM gold.fact_station_availability) f
        ON f.hour_local = t.hour_local
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT t.hour_local
    FROM gold.dim_time t
    LEFT ANTI JOIN (SELECT DISTINCT hour_local FROM gold.fact_station_availability) f
        ON f.hour_local = t.hour_local
    WHERE t.hour_local < '2025-10-29 22:00:00'
    ORDER BY t.hour_local
""").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
