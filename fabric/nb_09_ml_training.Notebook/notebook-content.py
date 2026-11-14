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

# PASO 1.1 - IMPORTS, EXPERIMENTO DE MLFLOW Y COMPROBACION DE LIBRERIAS
# Que hace: importa las librerias, desactiva el registro automatico de MLflow (para guardar
# nosotros a mano que se registra en cada ejecucion y entender por que), y crea el experimento
# 'exp_bicing_empty_next'. En Fabric, crear el experimento lo hace aparecer como item en el workspace.
# Tambien imprime las versiones y comprueba si LightGBM esta disponible.
import mlflow
import numpy as np
import pandas as pd
import sklearn
from pyspark.sql import functions as F

mlflow.autolog(disable=True)
mlflow.set_experiment("exp_bicing_empty_next")

print("mlflow:", mlflow.__version__)
print("scikit-learn:", sklearn.__version__)
try:
    import lightgbm
    print("lightgbm:", lightgbm.__version__)
except ImportError as e:
    print("lightgbm NO disponible:", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.2 - PARAMETROS Y FUNCION preparar() 
# Que hace: calcula con el TRAIN COMPLETO la mediana de altitude (una fila por estacion) y la lista
# fija de distritos, y define preparar(), que rellena los nulos de altitude y convierte district
# en columnas 0/1. 
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

print("mediana de altitude:", mediana_altitud)
print("distritos:", len(distritos), distritos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.3 - CARGA DE TRAIN / VALIDATION / TEST, PREPARACION Y SEPARACION X / y
# Que hace: lee las tres particiones de gold, convierte las columnas decimal a double (si no,
# pandas las trae como objetos y los modelos fallan), las pasa a pandas y aplica preparar().
# Separa la etiqueta (empty_next) de las caracteristicas.
# Columnas que NO se usan como caracteristicas:
#   station_id, hour_local -> identificadores, no explican nada por si solos
#   month -> decision del proyecto: el train cubre nov-ago, validacion es septiembre y test octubre,
#            asi que el modelo nunca veria esos meses; con un solo ano un mes no es una causa fiable.
# Al final cuenta los nulos por columna para ver que hay que imputar.
DEC = ["temperature_c", "precipitation_mm", "wind_ms"]
ETIQUETA = "empty_next"
NO_USAR = ["station_id", "hour_local", "month"]

def cargar(tabla):
    sdf = spark.table(tabla)
    for c in DEC:
        sdf = sdf.withColumn(c, F.col(c).cast("double"))
    pdf = sdf.toPandas()
    print(tabla, "-> nulos en la etiqueta:", pdf[ETIQUETA].isna().sum())
    pdf = preparar(pdf, mediana_altitud, distritos)
    y = pdf[ETIQUETA].astype(int)
    X = pdf.drop(columns=[ETIQUETA] + NO_USAR).astype(float)
    return X, y

X_train, y_train = cargar("gold.ml_train")
X_val, y_val = cargar("gold.ml_validation")
X_test, y_test = cargar("gold.ml_test")

print("train:", X_train.shape, "| validation:", X_val.shape, "| test:", X_test.shape)
print("columnas:", list(X_train.columns))
print("%% de empty_next=1 -> train: %.1f | validation: %.1f | test: %.1f"
      % (100 * y_train.mean(), 100 * y_val.mean(), 100 * y_test.mean()))
nulos = X_train.isna().sum()
print("nulos por columna en train:")
print(nulos[nulos > 0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.4 - FUNCIONES DE EVALUACION Y EJECUCION DE REFERENCIA (BASELINE) EN MLFLOW
# Que hace:
#  - evaluar(): calcula F1, precision, recall y AUC a partir de las probabilidades de un modelo.
#  - mejor_umbral(): busca el umbral de decision que maximiza el F1; SIEMPRE se calcula con
#    validacion (nunca con test) y luego se aplica tal cual al test.
#  - Registra en MLflow una ejecucion (run) 'baseline_persistence' con la regla de persistencia
#    medida sobre VALIDATION, para comparar los modelos con la misma vara de medir.
from sklearn.metrics import (precision_score, recall_score, f1_score,
                             roc_auc_score, precision_recall_curve)

def evaluar(y_true, proba, umbral=0.5):
    pred = (proba >= umbral).astype(int)
    return {
        "f1": f1_score(y_true, pred),
        "precision": precision_score(y_true, pred),
        "recall": recall_score(y_true, pred),
        "roc_auc": roc_auc_score(y_true, proba),
    }

def mejor_umbral(y_true, proba):
    p, r, t = precision_recall_curve(y_true, proba)
    f1 = 2 * p * r / (p + r + 1e-12)
    return float(t[np.argmax(f1[:-1])])

with mlflow.start_run(run_name="baseline_persistence"):
    mlflow.log_param("modelo", "persistencia: empty_next = (empty_readings > 0)")
    mlflow.log_param("conjunto_evaluacion", "validation")
    proba_base = (X_val["empty_readings"] > 0).astype(float)
    m = evaluar(y_val, proba_base)
    mlflow.log_metrics({"val_" + k: v for k, v in m.items()})
    print("baseline en validation:", {k: round(v, 4) for k, v in m.items()})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.5 - FUNCION entrenar_y_registrar() Y MODELO 1: REGRESION LOGISTICA
# Que hace: entrena un modelo, lo evalua en validacion y registra en MLflow (un 'run'):
#   - params: los ajustes del modelo
#   - metrics: train_f1_umbral05 (ajuste en entrenamiento, sobre una muestra fija),
#              val_f1_umbral05 (umbral estandar 0,5), val_f1 / precision / recall (con el umbral que
#              maximiza el F1 EN VALIDACION), val_roc_auc y umbral_optimo
#   - el modelo entrenado como artefacto, para poder registrarlo despues
# La regresion logistica no admite nulos ni escalas distintas: el Pipeline rellena los nulos con la
# mediana (calculada solo con train) y escala las columnas antes de ajustar.
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from mlflow.models import infer_signature

muestra_tr = X_train.sample(300_000, random_state=42).index

def entrenar_y_registrar(nombre, modelo, params):
    with mlflow.start_run(run_name=nombre):
        mlflow.log_params(params)
        modelo.fit(X_train, y_train)
        p_tr = modelo.predict_proba(X_train.loc[muestra_tr])[:, 1]
        p_val = modelo.predict_proba(X_val)[:, 1]
        umbral = mejor_umbral(y_val, p_val)
        m05 = evaluar(y_val, p_val, 0.5)
        mopt = evaluar(y_val, p_val, umbral)
        mlflow.log_metrics({
            "train_f1_umbral05": f1_score(y_train.loc[muestra_tr], (p_tr >= 0.5).astype(int)),
            "val_f1_umbral05": m05["f1"],
            "val_f1": mopt["f1"],
            "val_precision": mopt["precision"],
            "val_recall": mopt["recall"],
            "val_roc_auc": mopt["roc_auc"],
            "umbral_optimo": umbral,
        })
        mlflow.sklearn.log_model(modelo, artifact_path="model",
                                 signature=infer_signature(X_val.head(100), p_val[:100]))
        print(nombre, "| val F1 (umbral optimo %.3f): %.4f | val F1 (umbral 0,5): %.4f | AUC: %.4f"
              % (umbral, mopt["f1"], m05["f1"], mopt["roc_auc"]))
    return modelo

logreg = Pipeline([
    ("imputar", SimpleImputer(strategy="median")),
    ("escalar", StandardScaler()),
    ("clf", LogisticRegression(C=1.0, max_iter=300)),
])
logreg = entrenar_y_registrar(
    "logreg", logreg,
    {"modelo": "LogisticRegression", "C": 1.0, "max_iter": 300,
     "imputacion": "mediana (train)", "escalado": "StandardScaler"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.6 - MODELO 2: LIGHTGBM, TRES VARIANTES
# Que hace: entrena un gradient boosting de arboles (LightGBM) con tres configuraciones que cambian
# la complejidad de cada arbol (num_leaves) y el ritmo de aprendizaje, y registra cada una en MLflow
# con la misma funcion de antes. LightGBM admite nulos de forma nativa: no hace falta imputar.
from lightgbm import LGBMClassifier

variantes = {
    "lgbm_v1_pequeno": dict(num_leaves=15,  learning_rate=0.10, n_estimators=300, min_child_samples=20),
    "lgbm_v2_medio":   dict(num_leaves=63,  learning_rate=0.05, n_estimators=500, min_child_samples=20),
    "lgbm_v3_grande":  dict(num_leaves=255, learning_rate=0.05, n_estimators=500, min_child_samples=200),
}

modelos_lgbm = {}
for nombre, hp in variantes.items():
    modelo = LGBMClassifier(random_state=42, n_jobs=-1, verbose=-1, **hp)
    modelos_lgbm[nombre] = entrenar_y_registrar(nombre, modelo, {"modelo": "LGBMClassifier", **hp})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.7 - MODELO 3: RANDOM FOREST
# Que hace: entrena un random forest (muchos arboles independientes cuyas predicciones se promedian).
# Con 3,8 M de filas se limita para que no tarde ni pese demasiado:
#   max_samples=0.3 -> cada arbol se construye con el 30% de las filas (bootstrap)
#   max_depth=16, min_samples_leaf=200 -> arboles acotados
# scikit-learn 1.6 admite nulos en el random forest, asi que no se imputa.
from sklearn.ensemble import RandomForestClassifier

hp_rf = dict(n_estimators=100, max_depth=16, min_samples_leaf=200, max_samples=0.3)
rf = RandomForestClassifier(random_state=42, n_jobs=-1, **hp_rf)
rf = entrenar_y_registrar("rf_v1", rf, {"modelo": "RandomForestClassifier", **hp_rf})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.7b - MODELO 3 (VARIANTE 2): RANDOM FOREST CON MAS CAPACIDAD
# Que hace: mismo random forest, pero con menos restricciones que rf_v1 para que la comparacion con
# LightGBM sea mas equilibrada: mas arboles, mas filas por arbol, mas profundidad y hojas mas pequenas.
# Ojo: mas hojas = modelo mas pesado al guardarlo en MLflow, por eso no se sube mas.
hp_rf2 = dict(n_estimators=150, max_depth=20, min_samples_leaf=100, max_samples=0.5)
rf2 = RandomForestClassifier(random_state=42, n_jobs=-1, **hp_rf2)
rf2 = entrenar_y_registrar("rf_v2", rf2, {"modelo": "RandomForestClassifier", **hp_rf2})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.8 - COMPARACION DE TODOS LOS RUNS DEL EXPERIMENTO
# Que hace: lee de MLflow todas las ejecuciones de 'exp_bicing_empty_next' y las ordena por
# val_f1 (F1 en validacion con el umbral optimo). Compara train_f1_umbral05 con val_f1_umbral05
# para detectar sobreajuste: si el de train es mucho mayor, el modelo memoriza en vez de generalizar.
runs = mlflow.search_runs(experiment_names=["exp_bicing_empty_next"])

tabla = pd.DataFrame({
    "run": runs["tags.mlflow.runName"],
    "val_f1": runs["metrics.val_f1"],
    "val_f1_0.5": runs["metrics.val_f1_umbral05"],
    "train_f1_0.5": runs["metrics.train_f1_umbral05"],
    "val_precision": runs["metrics.val_precision"],
    "val_recall": runs["metrics.val_recall"],
    "val_auc": runs["metrics.val_roc_auc"],
    "umbral": runs["metrics.umbral_optimo"],
    "segundos": (runs["end_time"] - runs["start_time"]).dt.total_seconds().round(0),
})
tabla = tabla.sort_values("val_f1", ascending=False).round(4)
print(tabla.to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.9 - NUEVA CARACTERISTICA: TASA HISTORICA DE HORAS VACIAS POR ESTACION
# Que hace: anade a los tres conjuntos 'tasa_vacia_estacion' = porcentaje de horas con alguna lectura
# vacia (empty_readings > 0) de cada estacion, calculado SOLO con TRAIN (con validation o test seria
# fuga de datos). Una estacion que no aparezca en train recibe nulo (LightGBM y el random forest
# lo admiten). Se vuelve a cargar cada particion para conservar station_id, que antes se descarto.
# Los conjuntos sin la caracteristica se guardan como *_base para poder comparar y volver atras.
X_train_base, y_train_base = X_train, y_train
X_val_base, y_val_base = X_val, y_val
X_test_base, y_test_base = X_test, y_test

def cargar_con_id(tabla):
    sdf = spark.table(tabla)
    for c in DEC:
        sdf = sdf.withColumn(c, F.col(c).cast("double"))
    pdf = sdf.toPandas()
    ids = pdf["station_id"].copy()
    pdf = preparar(pdf, mediana_altitud, distritos)
    y = pdf[ETIQUETA].astype(int)
    X = pdf.drop(columns=[ETIQUETA] + NO_USAR).astype(float)
    return X, y, ids

X_train, y_train, id_train = cargar_con_id("gold.ml_train")
X_val, y_val, id_val = cargar_con_id("gold.ml_validation")
X_test, y_test, id_test = cargar_con_id("gold.ml_test")

tasa = (X_train["empty_readings"] > 0).groupby(id_train).mean()
for X, ids in [(X_train, id_train), (X_val, id_val), (X_test, id_test)]:
    X["tasa_vacia_estacion"] = ids.map(tasa).astype(float)

print("train:", X_train.shape, "| validation:", X_val.shape, "| test:", X_test.shape)
print("estaciones con tasa (train):", len(tasa))
print("tasa por estacion -> min %.3f | mediana %.3f | max %.3f" % (tasa.min(), tasa.median(), tasa.max()))
print("nulos en tasa_vacia_estacion -> train: %d | validation: %d | test: %d"
      % (X_train["tasa_vacia_estacion"].isna().sum(),
         X_val["tasa_vacia_estacion"].isna().sum(),
         X_test["tasa_vacia_estacion"].isna().sum()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.10 - LIGHTGBM CON LA NUEVA CARACTERISTICA (variantes v1 y v2)
# Que hace: repite lgbm_v1_pequeno y lgbm_v2_medio con los MISMOS hiperparametros, pero ahora X_train,
# X_val y X_test llevan tambien tasa_vacia_estacion. Se registran con el sufijo _con_tasa y el
# parametro con_tasa_estacion=True, para compararlos con los runs sin ella en el mismo experimento.
for nombre in ["lgbm_v1_pequeno", "lgbm_v2_medio"]:
    hp = variantes[nombre]
    modelo = LGBMClassifier(random_state=42, n_jobs=-1, verbose=-1, **hp)
    entrenar_y_registrar(nombre + "_con_tasa", modelo,
                         {"modelo": "LGBMClassifier", "con_tasa_estacion": True, **hp})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.11 - REGISTRAR EL MODELO ELEGIDO (lgbm_v3_grande) EN EL REGISTRO DE MODELOS
# Que hace: busca en el experimento el run 'lgbm_v3_grande' (sin la caracteristica tasa_vacia_estacion),
# recupera el umbral optimo que se fijo con VALIDATION y registra el modelo guardado en ese run como
# 'ml_bicing_empty_next'. En Fabric, esto crea un item 'ML model' en el workspace con su version 1.
runs = mlflow.search_runs(experiment_names=["exp_bicing_empty_next"],
                          filter_string="tags.mlflow.runName = 'lgbm_v3_grande'")
print("runs encontrados con ese nombre:", len(runs))
run_id = runs.iloc[0]["run_id"]
umbral_elegido = float(runs.iloc[0]["metrics.umbral_optimo"])

modelo_reg = mlflow.register_model(f"runs:/{run_id}/model", "ml_bicing_empty_next")
print("modelo registrado:", modelo_reg.name, "| version:", modelo_reg.version)
print("run_id:", run_id)
print("umbral elegido (validacion):", round(umbral_elegido, 4))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 1.12 - EVALUACION FINAL SOBRE TEST (UNA SOLA VEZ)
# Que hace: carga el modelo registrado, lo aplica a TEST con el umbral fijado en VALIDATION, mide la
# referencia (persistencia) sobre el mismo test y registra todo en un run 'final_test_lgbm_v3_grande'.
# Usa X_test_base (24 columnas, sin tasa_vacia_estacion): es con lo que se entreno el modelo.
# IMPORTANTE: el test se usa una sola vez; si se ajusta el modelo mirando estos numeros, deja de ser imparcial.
from sklearn.metrics import accuracy_score, confusion_matrix

modelo_final = mlflow.sklearn.load_model("models:/ml_bicing_empty_next/1")

p_test = modelo_final.predict_proba(X_test_base)[:, 1]
pred_modelo = (p_test >= umbral_elegido).astype(int)
m_modelo = evaluar(y_test_base, p_test, umbral_elegido)
acc_modelo = accuracy_score(y_test_base, pred_modelo)

pred_base = (X_test_base["empty_readings"] > 0).astype(int)
m_base = evaluar(y_test_base, pred_base.astype(float))
acc_base = accuracy_score(y_test_base, pred_base)
acc_trivial = 1 - y_test_base.mean()

tn, fp, fn, tp = confusion_matrix(y_test_base, pred_modelo).ravel()

with mlflow.start_run(run_name="final_test_lgbm_v3_grande"):
    mlflow.log_param("modelo_registrado", "ml_bicing_empty_next v1")
    mlflow.log_param("umbral", umbral_elegido)
    mlflow.log_metrics({
        "test_f1": m_modelo["f1"], "test_precision": m_modelo["precision"],
        "test_recall": m_modelo["recall"], "test_roc_auc": m_modelo["roc_auc"],
        "test_accuracy": acc_modelo,
        "test_baseline_f1": m_base["f1"], "test_baseline_accuracy": acc_base,
    })

print("TEST | filas:", len(y_test_base), "| % vacias:", round(100 * y_test_base.mean(), 1))
print("modelo   -> F1 %.4f | precision %.4f | recall %.4f | AUC %.4f | acierto %.4f"
      % (m_modelo["f1"], m_modelo["precision"], m_modelo["recall"], m_modelo["roc_auc"], acc_modelo))
print("baseline -> F1 %.4f | precision %.4f | recall %.4f | acierto %.4f"
      % (m_base["f1"], m_base["precision"], m_base["recall"], acc_base))
print("acierto diciendo siempre 'no vacia': %.4f" % acc_trivial)
print("matriz del modelo -> verdaderos negativos:", tn, "| falsos positivos:", fp,
      "| falsos negativos:", fn, "| verdaderos positivos:", tp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
