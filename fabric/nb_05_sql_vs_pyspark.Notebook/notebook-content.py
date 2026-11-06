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

def fmt(n):
    return f"{n:,}".replace(",", ".")

df = spark.table("bronze.station_info")

print("filas                     ", fmt(df.count()))
print("dropDuplicates por clave  ", fmt(df.dropDuplicates(["station_id", "last_updated"]).count()))
print("distinct() con todo       ", fmt(df.distinct().count()))
print("distinct() sin traza      ", fmt(df.drop("source_file", "loaded_at").distinct().count()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

octubre = df.filter(F.col("source_file").contains("2025-10"))

print("filas octubre          ", fmt(octubre.count()))
print("altitude = 'NA'        ", fmt(octubre.filter(F.col("altitude") == "NA").count()))
print("altitude cast a double ", fmt(octubre.filter(F.col("altitude").cast("double").isNull()).count()))
print("rental_uris = 'NA'     ", fmt(octubre.filter(F.col("rental_uris") == "NA").count()))
print("rental_uris cast a int ", fmt(octubre.filter(F.col("rental_uris").cast("int").isNull()).count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
