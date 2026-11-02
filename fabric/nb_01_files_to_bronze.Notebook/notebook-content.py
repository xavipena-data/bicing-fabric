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

base = "Files/bronze/bicing/status/"
files = sorted(f.name for f in notebookutils.fs.ls(base))
print(len(files), "files")
for n in files:
    print(n)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw = {n: spark.read.option("header", True).csv(base + n) for n in files}
for n, df in raw.items():
    print(n, len(df.columns))
    print(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = raw["status_2024-11.csv"]
df.select("station_id", "last_reported", "traffic", "V1").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy("V1").count().show()
df.groupBy("traffic").count().show()

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

bronze = parts[0]
for df in parts[1:]:
    bronze = bronze.unionByName(df, allowMissingColumns=True)

bronze = bronze.withColumn("loaded_at", F.current_timestamp())

print(len(bronze.columns))
print(bronze.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze.write.mode("overwrite").format("delta").saveAsTable("bronze.station_status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT source_file, COUNT(*) AS row_count
    FROM bronze.station_status
    GROUP BY source_file
    ORDER BY source_file
""").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT source_file,
           SUM(CASE WHEN V1 IS NOT NULL AND V1 <> 'NA' THEN 1 ELSE 0 END) AS v1_values,
           SUM(CASE WHEN traffic IS NOT NULL AND traffic <> 'NA' THEN 1 ELSE 0 END) AS traffic_values
    FROM bronze.station_status
    GROUP BY source_file
    ORDER BY source_file
""").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
