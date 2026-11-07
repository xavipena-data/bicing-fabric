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

df = (spark.read.json("Files/raw/weather/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("loaded_at", F.current_timestamp()))

df.printSchema()
print(df.count())
df.groupBy("codi_variable").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df.write
   .mode("overwrite")
   .format("delta")
   .saveAsTable("bronze.weather_xema"))

spark.table("bronze.weather_xema").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bw = spark.table("bronze.weather_xema")
print("filas:", bw.count())
print("ids distintos:", bw.select("id").distinct().count())
bw.groupBy("codi_estat").count().orderBy("codi_estat").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
