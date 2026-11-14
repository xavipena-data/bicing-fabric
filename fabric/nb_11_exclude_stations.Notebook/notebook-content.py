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

# PASO 1.1 - EXCLUIR LAS 6 ESTACIONES SIN DISTRITO DE LAS TABLAS GOLD
# Que hace: borra de gold.fact_station_availability y gold.dim_station las estaciones de prueba (536, 545),
# las del evento Copa America (542, 543) y las dos altas sin distrito en origen (588, 597).
#  - antes de borrar, guarda un resumen del hecho y cuenta cuantas filas se van a quitar
#  - despues de borrar, comprueba con asserts que lo que queda es exactamente lo anterior menos lo quitado
#  - ejecutar una sola vez; si se repite, no borra nada mas y las comprobaciones siguen pasando
from pyspark.sql import functions as F

ids = (536, 542, 543, 545, 588, 597)
COLS = ["readings", "empty_readings", "full_readings", "out_of_service_readings"]

def resumen_hecho(filtro=""):
    return spark.sql(f"""
        SELECT COUNT(*) AS filas,
               COALESCE(SUM(readings), 0)                AS readings,
               COALESCE(SUM(empty_readings), 0)          AS empty_readings,
               COALESCE(SUM(full_readings), 0)           AS full_readings,
               COALESCE(SUM(out_of_service_readings), 0) AS out_of_service_readings
        FROM gold.fact_station_availability {filtro}
    """).collect()[0].asDict()

antes = resumen_hecho()
quitadas = resumen_hecho(f"WHERE station_id IN {ids}")
dim_antes = spark.table("gold.dim_station").count()
dim_quitadas = spark.table("gold.dim_station").filter(F.col("station_id").isin(*ids)).count()

print("ANTES   | hecho:", antes, "| dim_station:", dim_antes)
print("QUITAR  | hecho:", quitadas, "| dim_station:", dim_quitadas)

spark.sql(f"DELETE FROM gold.fact_station_availability WHERE station_id IN {ids}")
spark.sql(f"DELETE FROM gold.dim_station WHERE station_id IN {ids}")

despues = resumen_hecho()
dim_despues = spark.table("gold.dim_station").count()
print("DESPUES | hecho:", despues, "| dim_station:", dim_despues)

for k in ["filas"] + COLS:
    assert antes[k] - quitadas[k] == despues[k], f"no cuadra {k}"
assert dim_antes - dim_quitadas == dim_despues, "no cuadra dim_station"
print("OK: lo que queda es lo anterior menos lo quitado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.2 - REFERENCIAS NUEVAS DE LA TASA DE VACIAS Y LA MEDIA DE BICIS TRAS LA EXCLUSION
# Calcula en Spark lo que deben dar las medidas DAX despues de refrescar el modelo.
spark.sql("""
    SELECT COUNT(DISTINCT station_id)                          AS active_stations,
           SUM(empty_readings) / SUM(readings)                 AS empty_rate,
           SUM(avg_bikes_available * readings) / SUM(readings) AS avg_bikes_available
    FROM gold.fact_station_availability
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.3 - COLUMNA district_label CON LOS NOMBRES OFICIALES DE LOS DISTRITOS
# Que hace: añade a gold.dim_station una columna district_label (nombre oficial con espacios) calculada a partir
# de district_code, que si es estable. La columna district NO se toca: el modelo de ML se entreno con esos nombres.
#  - si la columna ya existe (celda repetida), no la vuelve a crear
#  - al final comprueba que ninguna estacion se queda sin etiqueta y muestra la correspondencia
if "district_label" not in spark.table("gold.dim_station").columns:
    spark.sql("ALTER TABLE gold.dim_station ADD COLUMNS (district_label STRING)")

spark.sql("""
    UPDATE gold.dim_station
    SET district_label = CASE district_code
        WHEN '01' THEN 'Ciutat Vella'
        WHEN '02' THEN 'Eixample'
        WHEN '03' THEN 'Sants-Montjuïc'
        WHEN '04' THEN 'Les Corts'
        WHEN '05' THEN 'Sarrià-Sant Gervasi'
        WHEN '06' THEN 'Gràcia'
        WHEN '07' THEN 'Horta-Guinardó'
        WHEN '08' THEN 'Nou Barris'
        WHEN '09' THEN 'Sant Andreu'
        WHEN '10' THEN 'Sant Martí'
    END
""")

dim = spark.table("gold.dim_station")
sin_etiqueta = dim.filter("district_label IS NULL").count()
print("estaciones sin etiqueta:", sin_etiqueta, "| total:", dim.count())
assert sin_etiqueta == 0, "hay estaciones sin district_label"

dim.groupBy("district_code", "district", "district_label").count().orderBy("district_code").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
