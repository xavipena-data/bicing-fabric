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

spark.conf.set("spark.sql.session.timeZone", "UTC")

bw = spark.table("bronze.weather_xema")

silver = (bw
    .filter(F.col("codi_base") == "SH")
    .select(
        F.col("id").alias("source_id"),
        F.col("codi_estacio").alias("station_code"),
        F.col("codi_variable").alias("variable_code"),
        F.when(F.col("codi_variable") == "32", "temperature_c")
         .when(F.col("codi_variable") == "35", "precipitation_mm")
         .when(F.col("codi_variable") == "30", "wind_ms")
         .alias("variable_name"),
        F.to_timestamp("data_lectura").alias("reading_utc"),
        F.col("valor_lectura").cast("decimal(10,2)").alias("value"),
        F.coalesce(F.col("codi_estat"), F.lit("N")).alias("validation_status"),
    )
    .withColumn("reading_local", F.from_utc_timestamp("reading_utc", "Europe/Madrid"))
)

print("filas:", silver.count())
print("value nulo:", silver.filter(F.col("value").isNull()).count())
print("reading_utc nulo:", silver.filter(F.col("reading_utc").isNull()).count())
print("variable_name nulo:", silver.filter(F.col("variable_name").isNull()).count())
silver.orderBy("reading_utc").show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for d1, d2 in [("2025-03-30 00:00:00", "2025-03-30 01:30:00"),
               ("2025-10-26 00:00:00", "2025-10-26 01:30:00")]:
    (silver.filter((F.col("variable_code") == "32") &
                   F.col("reading_utc").between(d1, d2))
           .select("reading_utc", "reading_local", "value")
           .orderBy("reading_utc").show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(silver.write
   .mode("overwrite")
   .format("delta")
   .saveAsTable("silver.weather_xema_30min"))

spark.table("silver.weather_xema_30min").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hourly = (spark.table("silver.weather_xema_30min")
    .withColumn("hour_local", F.date_trunc("hour", "reading_local"))
    .groupBy("hour_local")
    .agg(
        F.round(F.avg(F.when(F.col("variable_name") == "temperature_c", F.col("value"))), 2).alias("temperature_c"),
        F.sum(F.when(F.col("variable_name") == "precipitation_mm", F.col("value"))).alias("precipitation_mm"),
        F.round(F.avg(F.when(F.col("variable_name") == "wind_ms", F.col("value"))), 2).alias("wind_ms"),
        F.least(
            F.count(F.when(F.col("variable_name") == "temperature_c", 1)),
            F.count(F.when(F.col("variable_name") == "precipitation_mm", 1)),
            F.count(F.when(F.col("variable_name") == "wind_ms", 1)),
        ).alias("n_readings"),
    )
)

print("horas:", hourly.count())
hourly.agg(F.min("hour_local"), F.max("hour_local")).show(truncate=False)
hourly.groupBy("n_readings").count().orderBy("n_readings").show()
hourly.orderBy("hour_local").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dt = spark.table("gold.dim_time")
dt.printSchema()

dw = dt.select("hour_local").join(hourly, "hour_local", "left")

print("filas dim_weather:", dw.count())
print("horas sin tiempo:", dw.filter(F.col("n_readings").isNull()).count())
dw.filter(F.col("n_readings").isNull()).orderBy("hour_local").show(30, truncate=False)

print("horas de tiempo fuera de dim_time:", hourly.join(dt.select("hour_local"), "hour_local", "left_anti").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_weather = (dw
    .withColumn("n_readings", F.coalesce(F.col("n_readings"), F.lit(0)).cast("int"))
    .select("hour_local", "temperature_c", "precipitation_mm", "wind_ms", "n_readings")
)

(dim_weather.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_weather"))

t = spark.table("gold.dim_weather")
print("filas:", t.count(), "| horas distintas:", t.select("hour_local").distinct().count())
t.groupBy("n_readings").count().orderBy("n_readings").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
