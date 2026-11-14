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

# PASO 1.1 - IMPORTS, PARAMETROS DE PREPARACION, preparar(), UMBRAL Y MODELO REGISTRADO
# Que hace: deja listo todo lo necesario para predecir:
#  - repite el calculo de la mediana de altitude y de la lista de distritos con el TRAIN completo
#    y define preparar(), igual que en nb_09 (los notebooks no comparten variables)
#  - recupera el umbral que se fijo con VALIDATION, leyendolo del run 'lgbm_v3_grande' de MLflow
#    en vez de escribirlo a mano
#  - carga desde el registro de modelos 'ml_bicing_empty_next' version 1
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

train = spark.table("gold.ml_train")
est = train.select("station_id", "altitude").dropDuplicates(["station_id"])
mediana_altitud = est.approxQuantile("altitude", [0.5], 0.0)[0]
distritos = sorted(r["district"] for r in train.select("district").distinct().collect())

def preparar(df, mediana_altitud, distritos):
    out = df.copy()
    out["altitude"] = out["altitude"].fillna(mediana_altitud)
    cat = pd.Categorical(out["district"], categories=distritos)
    dummies = pd.get_dummies(cat, prefix="district", dtype=int)
    dummies.index = out.index
    pos = out.columns.get_loc("district")
    return pd.concat([out.iloc[:, :pos], dummies, out.iloc[:, pos + 1:]], axis=1)

runs = mlflow.search_runs(experiment_names=["exp_bicing_empty_next"],
                          filter_string="tags.mlflow.runName = 'lgbm_v3_grande'")
umbral = float(runs.iloc[0]["metrics.umbral_optimo"])
modelo = mlflow.sklearn.load_model("models:/ml_bicing_empty_next/1")

print("mediana de altitude:", mediana_altitud)
print("distritos:", len(distritos))
print("umbral:", round(umbral, 4))
print("modelo cargado:", type(modelo).__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.2 - PREDICCION POR LOTES SOBRE VALIDATION Y TEST Y ESCRITURA DE gold.station_predictions
# Que hace: genera las predicciones del modelo registrado para las filas que NO se usaron al entrenar.
#  - cargar_split(): lee la tabla, castea los decimales a double, la pasa a pandas, aplica preparar()
#    y conserva station_id y hour_local (las claves de las relaciones del modelo semantico)
#  - la matriz X lleva exactamente las 24 columnas del entrenamiento y en el mismo orden
#    (se comprueba contra las columnas que guardo el propio modelo)
#  - empty_probability = predict_proba; empty_predicted = probabilidad >= umbral fijado con validation
#  - empty_actual = lo que ocurrio de verdad, para poder medir el modelo en el informe
#  - se escribe como tabla Delta (overwrite: se puede relanzar la celda sin duplicar filas)
DEC = ["temperature_c", "precipitation_mm", "wind_ms"]
QUITAR = ["empty_next", "station_id", "hour_local", "month"]

def cargar_split(tabla, nombre_split):
    sdf = spark.table(tabla)
    for c in DEC:
        sdf = sdf.withColumn(c, F.col(c).cast("double"))
    pdf = preparar(sdf.toPandas(), mediana_altitud, distritos)
    X = pdf.drop(columns=QUITAR).astype(float)
    # mismo orden de columnas que vio el modelo al entrenar
    assert list(X.columns) == list(modelo.feature_name_), "Las columnas no coinciden con las del modelo"
    prob = modelo.predict_proba(X)[:, 1]
    return pd.DataFrame({
        "station_id": pdf["station_id"].values,
        "hour_local": pdf["hour_local"].values,
        "split": nombre_split,
        "empty_probability": prob,
        "empty_predicted": (prob >= umbral).astype(int),
        "empty_actual": pdf["empty_next"].astype(int).values,
    })

pred_val = cargar_split("gold.ml_validation", "validation")
pred_test = cargar_split("gold.ml_test", "test")
pred = pd.concat([pred_val, pred_test], ignore_index=True)

print("validation:", len(pred_val), "| test:", len(pred_test), "| total:", len(pred))
print("columnas del modelo:", len(modelo.feature_name_))

(spark.createDataFrame(pred)
      .write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("gold.station_predictions"))

print("tabla gold.station_predictions escrita")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.3 - VERIFICACION DE gold.station_predictions LEYENDOLA DESDE EL LAKEHOUSE
# Que hace: comprueba que lo guardado es integro y reproduce las metricas de MLflow.
#  - filas por split
#  - nulos en las columnas y duplicados en la clave (station_id, hour_local)
#  - F1, precision y recall recalculados sobre la tabla, por split
#  - tasa real de vacias vs tasa predicha, por split (sanity check del umbral)
from sklearn.metrics import f1_score, precision_score, recall_score

chk = spark.table("gold.station_predictions")

print("--- filas por split ---")
chk.groupBy("split").count().orderBy("split").show()

print("--- nulos por columna ---")
chk.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in chk.columns]).show()

dup = chk.groupBy("station_id", "hour_local", "split").count().filter("count > 1").count()
print("duplicados en (station_id, hour_local, split):", dup)

p = chk.toPandas()
print("--- metricas recalculadas sobre la tabla ---")
for s in ["validation", "test"]:
    d = p[p["split"] == s]
    print(s,
          "| F1:", round(f1_score(d["empty_actual"], d["empty_predicted"]), 4),
          "| precision:", round(precision_score(d["empty_actual"], d["empty_predicted"]), 4),
          "| recall:", round(recall_score(d["empty_actual"], d["empty_predicted"]), 4),
          "| real vacias:", round(d["empty_actual"].mean(), 4),
          "| predichas vacias:", round(d["empty_predicted"].mean(), 4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.4 - TIPOS DE gold.station_predictions vs dim_time
# Que hace: compara el tipo de hour_local en la tabla nueva y en dim_time, y muestra el resto de tipos.
# Direct Lake no admite TIMESTAMP_NTZ, y las columnas de una relacion deben tener el mismo tipo.
spark.sql("DESCRIBE gold.station_predictions").show(truncate=False)
spark.sql("DESCRIBE gold.dim_time").filter("col_name = 'hour_local'").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.5 - CONTRASTE DE LAS MEDIDAS BASE DEL MODELO CON LA TABLA DELTA
# Que hace: calcula en Spark las mismas cinco cifras que las medidas DAX, directamente sobre gold.fact_station_availability.
# Si coinciden, el modelo semantico suma exactamente lo que hay en el lakehouse.
spark.sql("""
    SELECT SUM(readings)                  AS readings,
           SUM(empty_readings)            AS empty_readings,
           SUM(full_readings)             AS full_readings,
           SUM(out_of_service_readings)   AS out_of_service_readings,
           COUNT(DISTINCT station_id)     AS active_stations
    FROM gold.fact_station_availability
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.6 - CONTRASTE DE TASAS Y MEDIAS PONDERADAS CON LA TABLA DELTA
# Calcula en Spark las mismas cinco cifras que las medidas DAX del paso 11.
spark.sql("""
    SELECT SUM(empty_readings) / SUM(readings)                 AS empty_rate,
           SUM(full_readings) / SUM(readings)                  AS full_rate,
           SUM(out_of_service_readings) / SUM(readings)        AS out_of_service_rate,
           SUM(avg_bikes_available * readings) / SUM(readings) AS avg_bikes_available,
           SUM(avg_docks_available * readings) / SUM(readings) AS avg_docks_available
    FROM gold.fact_station_availability
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.7 - FILAS DE LAS 6 ESTACIONES SIN DISTRITO EN LAS TABLAS GOLD
# Que hace: cuenta las filas de esas 6 estaciones (536, 542, 543, 545, 588, 597) en el hecho de gold,
# en la tabla base de ML y en sus tres particiones, para saber que habria que tocar antes de borrar nada.
ids = (536, 542, 543, 545, 588, 597)
for tabla in ["gold.dim_station", "gold.fact_station_availability", "gold.ml_station_hour",
              "gold.ml_train", "gold.ml_validation", "gold.ml_test"]:
    print("---", tabla)
    spark.sql(f"""
        SELECT station_id, COUNT(*) AS filas
        FROM {tabla}
        WHERE station_id IN {ids}
        GROUP BY station_id
        ORDER BY station_id
    """).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
