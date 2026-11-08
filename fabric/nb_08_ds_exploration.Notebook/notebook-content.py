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

import matplotlib.pyplot as plt
from cycler import cycler

plt.rcParams.update({
    "axes.prop_cycle": cycler(color=["#F5C518", "#0D0D0D", "#8C8C8C"]),
    "axes.edgecolor": "#8C8C8C",
    "axes.labelcolor": "#0D0D0D",
    "axes.titlecolor": "#0D0D0D",
    "xtick.color": "#8C8C8C",
    "ytick.color": "#8C8C8C",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": "#E6E6E6",
    "figure.facecolor": "#FFFFFF",
    "axes.facecolor": "#FFFFFF",
    "font.family": "sans-serif",
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.3 - CONSTRUIR EL CONJUNTO DE MODELADO (una fila = estacion x hora t)
# 1) Etiqueta empty_next: 1 si en la hora t+1 hubo alguna lectura con la estacion vacia.
# 2) Historia: bicis medias hace 1 h (lag1) y hace 24 h (lag24).
# 3) Calendario (dim_time), estacion (dim_station) y tiempo de la hora t (dim_weather).
# 4) Capacidad vigente en la hora t (tramo valid_from/valid_to de station_capacity).
# 5) Revision: filas, proporcion de la etiqueta y nulos por columna.


from pyspark.sql import functions as F

fact = spark.table("gold.fact_station_availability")

fact.select(
    F.count("*").alias("station_hours"),
    F.avg((F.col("empty_readings") > 0).cast("int")).alias("p_any_empty"),
    F.avg((F.col("full_readings") > 0).cast("int")).alias("p_any_full"),
    F.avg((F.col("empty_readings") == F.col("readings")).cast("int")).alias("p_all_empty"),
    F.avg((F.col("full_readings") == F.col("readings")).cast("int")).alias("p_all_full"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cap = spark.table("gold.station_capacity")
cap.printSchema()
cap.show(5, truncate=False)
print("filas:", cap.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")

fact = (spark.table("gold.fact_station_availability")
        .select("station_id", "hour_local", "avg_bikes_available", "avg_docks_available",
                "empty_readings", "full_readings"))
print("filas en la tabla de hechos:", fact.count())

# 1) Etiqueta: lo que pasa en t+1, desplazado una hora hacia atras para alinearlo con t
label = fact.select(
    "station_id",
    (F.col("hour_local") - F.expr("INTERVAL 1 HOUR")).alias("hour_local"),
    (F.col("empty_readings") > 0).cast("int").alias("empty_next"))
ds = fact.join(label, ["station_id", "hour_local"], "inner")
print("filas con hora t+1:", ds.count())

# 2) Historia: bicis medias hace 1 hora y hace 24 horas (se desplazan hacia delante para alinearlas con t)
def lag(hours, name):
    return fact.select(
        "station_id",
        (F.col("hour_local") + F.expr(f"INTERVAL {hours} HOURS")).alias("hour_local"),
        F.col("avg_bikes_available").alias(name))
ds = (ds.join(lag(1, "avg_bikes_lag1"), ["station_id", "hour_local"], "left")
        .join(lag(24, "avg_bikes_lag24"), ["station_id", "hour_local"], "left"))

# 3) Calendario, estacion y tiempo
cal = (spark.table("gold.dim_time")
       .select("hour_local", "hour_of_day", "day_of_week",
               F.col("is_weekend").cast("int").alias("is_weekend"), "month"))
st = spark.table("gold.dim_station").select("station_id", "altitude", "district")
wx = spark.table("gold.dim_weather").select("hour_local", "temperature_c", "precipitation_mm", "wind_ms")
ds = (ds.join(cal, "hour_local", "left")
        .join(st, "station_id", "left")
        .join(wx, "hour_local", "left"))

# 4) Capacidad vigente en la hora t (tramo de fechas)
cap = spark.table("gold.station_capacity").alias("c")
ds = (ds.alias("a")
        .join(cap, (F.col("a.station_id") == F.col("c.station_id")) &
                   (F.col("a.hour_local") >= F.col("c.valid_from")) &
                   (F.col("a.hour_local") < F.col("c.valid_to")), "left")
        .drop(F.col("c.station_id"), F.col("c.valid_from"), F.col("c.valid_to")))

# 5) Revision
print("filas del conjunto:", ds.count())
print("proporcion empty_next = 1:", ds.agg(F.avg("empty_next")).first()[0])
ds.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ds.columns]).show(vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.4 - DIAGNOSTICO (solo lectura)
# 1) Que estaciones tienen altitud nula. 2) Cuantas filas tienen capacity = 0
#    y que proporcion de empty_next tienen (estacion fuera de servicio).

# 1) Estaciones sin altitud
sin_alt = ds.filter(F.col("altitude").isNull()).select("station_id").distinct()
print("estaciones con altitude nula en el conjunto:", sin_alt.count())
spark.table("gold.dim_station").filter(F.col("altitude").isNull()) \
     .select("station_id", "name", "district").show(30, truncate=False)

# 2) Filas con capacity = 0
ds.groupBy((F.col("capacity") == 0).alias("capacidad_cero")) \
  .agg(F.count("*").alias("filas"), F.avg("empty_next").alias("prop_empty_next")) \
  .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.5 - FILTRAR Y GUARDAR gold.ml_station_hour
# 1) Excluye capacity = 0 (fuera de servicio) y district = Unassigned (estaciones temporales).
# 2) Guarda el resultado como tabla Delta (overwrite: se puede repetir la celda).
# 3) Comprueba leyendo la tabla guardada: filas, estaciones, nulos de altitud y rango de fechas.

# Excluir: capacity = 0 (fuera de servicio) y estaciones temporales Unassigned
ds_final = (ds.filter(F.col("capacity") > 0)
              .filter(F.col("district") != "Unassigned"))

print("filas antes:", ds.count())
print("filas despues:", ds_final.count())
print("proporcion empty_next = 1:", ds_final.agg(F.avg("empty_next")).first()[0])

# Guardar como tabla Delta en la capa gold
(ds_final.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.ml_station_hour"))

# Comprobacion leyendo la tabla guardada
chk = spark.table("gold.ml_station_hour")
print("filas en la tabla:", chk.count())
print("estaciones distintas:", chk.select("station_id").distinct().count())
print("estaciones con altitude nula:", chk.filter(F.col("altitude").isNull()).select("station_id").distinct().count())
print("rango de fechas:", chk.agg(F.min("hour_local"), F.max("hour_local")).first())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.6 - COMPROBACION DEL FILTRO (solo lectura)
# 1) Que estaciones Unassigned habia en el conjunto original y cuantas filas aportaban.
# 2) Que no queda ninguna en gold.ml_station_hour.
# 3) Las estaciones con altitud nula que quedan en la tabla guardada.

# 1) Que estaciones de Unassigned habia y cuantas filas tenian
(ds.filter(F.col("district") == "Unassigned")
   .groupBy("station_id").count().orderBy("station_id").show(20))

# 2) Quedan Unassigned en la tabla guardada?
chk = spark.table("gold.ml_station_hour")
print("filas Unassigned en la tabla:", chk.filter(F.col("district") == "Unassigned").count())

# 3) Las 19 estaciones con altitud nula que quedan
chk.filter(F.col("altitude").isNull()).groupBy("station_id", "district").count().orderBy("station_id").show(30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.7 - EXPLORACION: % de empty_next por hora del dia y laborable/fin de semana
# La hora es la de t; la etiqueta habla de t+1. Agrega en Spark y pasa a pandas solo 48 filas.

t = spark.table("gold.ml_station_hour")

por_hora = (t.groupBy("hour_of_day", "is_weekend")
              .agg(F.count("*").alias("filas"),
                   F.round(F.avg("empty_next") * 100, 1).alias("pct_empty_next"))
              .orderBy("is_weekend", "hour_of_day")
              .toPandas())

tabla = por_hora.pivot(index="hour_of_day", columns="is_weekend", values="pct_empty_next")
tabla.columns = ["laborable", "fin_de_semana"]
print(tabla.to_string())
tabla.plot(kind="line", marker="o", figsize=(9, 4), title="% de horas con estación vacía en t+1, según la hora t")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.8 - EXPLORACION: % de empty_next por tramo de altitud y por tramo de lluvia (hora t)
# Las estaciones sin altitud van en un tramo aparte ("sin dato").

t = spark.table("gold.ml_station_hour")

# Por tramos de altitud (metros). Las 19 estaciones sin altitud van aparte
alt = (t.withColumn("tramo_altitud",
          F.when(F.col("altitude").isNull(), "sin dato")
           .when(F.col("altitude") < 20, "a) < 20 m")
           .when(F.col("altitude") < 60, "b) 20-60 m")
           .when(F.col("altitude") < 120, "c) 60-120 m")
           .otherwise("d) >= 120 m"))
        .groupBy("tramo_altitud")
        .agg(F.countDistinct("station_id").alias("estaciones"),
             F.count("*").alias("filas"),
             F.round(F.avg("empty_next") * 100, 1).alias("pct_empty_next"))
        .orderBy("tramo_altitud"))
alt.show()

# Por lluvia en la hora t (mm)
lluvia = (t.withColumn("tramo_lluvia",
          F.when(F.col("precipitation_mm").isNull(), "sin dato")
           .when(F.col("precipitation_mm") == 0, "a) 0 mm")
           .when(F.col("precipitation_mm") < 1, "b) 0-1 mm")
           .otherwise("c) >= 1 mm"))
        .groupBy("tramo_lluvia")
        .agg(F.count("*").alias("filas"),
             F.round(F.avg("empty_next") * 100, 1).alias("pct_empty_next"))
        .orderBy("tramo_lluvia"))
lluvia.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 4.9 - BASELINE DE PERSISTENCIA
# Regla: si en la hora t hubo alguna lectura vacia, predigo que en t+1 tambien.
# Calcula la matriz de confusion (tp, fp, fn, tn) y accuracy / precision / recall / F1.
# Referencia: la accuracy de "nunca vacia". Se calcula sobre todo el ano; se recalculara en el test.

t = (spark.table("gold.ml_station_hour")
       .withColumn("pred", (F.col("empty_readings") > 0).cast("int")))

m = t.agg(
    F.sum(F.col("pred") * F.col("empty_next")).alias("tp"),
    F.sum(F.col("pred") * (1 - F.col("empty_next"))).alias("fp"),
    F.sum((1 - F.col("pred")) * F.col("empty_next")).alias("fn"),
    F.sum((1 - F.col("pred")) * (1 - F.col("empty_next"))).alias("tn")).first()

tp, fp, fn, tn = m["tp"], m["fp"], m["fn"], m["tn"]
total = tp + fp + fn + tn
print("tp (aciertos vacia):", tp, " fp (falsa alarma):", fp)
print("fn (se escapan):   ", fn, " tn (aciertos no vacia):", tn)
print("accuracy: ", round((tp + tn) / total, 4))
print("precision:", round(tp / (tp + fp), 4))
print("recall:   ", round(tp / (tp + fn), 4))
print("F1:       ", round(2 * tp / (2 * tp + fp + fn), 4))
print("accuracy de 'nunca vacia':", round(1 - (tp + fn) / total, 4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 5.1 - SEPARACION TEMPORAL EN TRAIN / VALIDACION / TEST
# train:      hour_local <  2025-08-31 23:00   (nov 2024 - ago 2025)
# validacion: 2025-09-01 00:00 <= hour_local < 2025-09-30 23:00   (septiembre)
# test:       hour_local >= 2025-10-01 00:00   (octubre, hasta el 29)
# Se descartan 31-ago 23:00 y 30-sep 23:00: su etiqueta (t+1) caeria en el bloque siguiente.
# Guarda tres tablas Delta en gold y comprueba filas, fechas y proporcion de empty_next.

t = spark.table("gold.ml_station_hour")

splits = {
    "gold.ml_train":      t.filter(F.col("hour_local") < "2025-08-31 23:00:00"),
    "gold.ml_validation": t.filter((F.col("hour_local") >= "2025-09-01 00:00:00") &
                                   (F.col("hour_local") <  "2025-09-30 23:00:00")),
    "gold.ml_test":       t.filter(F.col("hour_local") >= "2025-10-01 00:00:00"),
}

for name, df in splits.items():
    df.write.mode("overwrite").format("delta").saveAsTable(name)

for name in splits:
    c = spark.table(name)
    r = c.agg(F.count("*").alias("filas"),
              F.min("hour_local").alias("desde"),
              F.max("hour_local").alias("hasta"),
              F.avg("empty_next").alias("prop_empty_next")).first()
    print(name, "| filas:", r["filas"], "| desde:", r["desde"], "| hasta:", r["hasta"],
          "| empty_next:", round(r["prop_empty_next"], 4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 5.2 - MUESTRA PARA DATA WRANGLER: 50 estaciones completas, solo del TRAIN
# 1) Elige 50 estaciones al azar (semilla 42, para poder repetirlo) y se queda con todas sus horas.
# 2) Convierte las columnas decimales a double: pandas las leeria como objetos Decimal
#    y Data Wrangler no las trata bien como numeros.
# 3) Pasa a pandas (variable sample_pd) y muestra tamano, tipos y memoria.
from pyspark.sql.types import DecimalType

train = spark.table("gold.ml_train")
est = train.select("station_id").distinct().orderBy(F.rand(42)).limit(50)
muestra = train.join(est, "station_id", "inner")

for f in muestra.schema.fields:
    if isinstance(f.dataType, DecimalType):
        muestra = muestra.withColumn(f.name, F.col(f.name).cast("double"))

sample_pd = muestra.toPandas()
print("filas, columnas:", sample_pd.shape)
print("estaciones:", sample_pd["station_id"].nunique())
print("memoria (MB):", round(sample_pd.memory_usage(deep=True).sum() / 1e6, 1))
print(sample_pd.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 5.3 - PARAMETROS DE PREPARACION, CALCULADOS SOLO CON EL TRAIN COMPLETO
# 1) Mediana de altitude: sobre una fila por estacion (la altitud es un dato de la estacion,
#    no de cada hora; asi una estacion con mas horas no pesa mas).
# 2) Lista fija de distritos: define las columnas 0/1 que tendran train, validacion y test.
train = spark.table("gold.ml_train")

est = train.select("station_id", "altitude").dropDuplicates(["station_id"])
mediana_altitud = est.approxQuantile("altitude", [0.5], 0.0)[0]
distritos = sorted(r["district"] for r in train.select("district").distinct().collect())

print("estaciones en train:", est.count(), "| con altitude nula:", est.filter(F.col("altitude").isNull()).count())
print("mediana de altitude:", mediana_altitud)
print("distritos:", len(distritos), distritos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 5.4 - FUNCION preparar(): rellena altitude y convierte district en columnas 0/1
# Misma logica que genero Data Wrangler, pero con parametros fijos calculados en el train:
#   mediana_altitud -> el valor con el que se rellenan los nulos de altitude (no se recalcula por bloque)
#   distritos       -> lista fija de categorias; asi train, validacion y test tienen las mismas columnas
# Se prueba con sample_pd (que sigue sin modificar) y con un subconjunto de un solo distrito.
import pandas as pd

def preparar(df, mediana_altitud, distritos):
    out = df.copy()
    out["altitude"] = out["altitude"].fillna(mediana_altitud)
    cat = pd.Categorical(out["district"], categories=distritos)
    dummies = pd.get_dummies(cat, prefix="district", dtype=int)
    dummies.index = out.index
    pos = out.columns.get_loc("district")
    return pd.concat([out.iloc[:, :pos], dummies, out.iloc[:, pos + 1:]], axis=1)

prep = preparar(sample_pd, mediana_altitud, distritos)
cols_d = [c for c in prep.columns if c.startswith("district_")]
print("forma antes y despues:", sample_pd.shape, prep.shape)
print("altitude nulos antes y despues:", sample_pd["altitude"].isna().sum(), prep["altitude"].isna().sum())
print("suma de columnas district_* por fila (debe ser 1 en todas):")
print(prep[cols_d].sum(axis=1).value_counts())

# Un bloque con un solo distrito debe dar las mismas columnas
una = preparar(sample_pd[sample_pd["district"] == "Eixample"], mediana_altitud, distritos)
print("columnas con un solo distrito:", una.shape[1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
