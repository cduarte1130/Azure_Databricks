# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType

# Configurar credenciales para ADLS Gen2
spark.conf.set(
    "fs.azure.account.key.adlsdatabrickscmd01.dfs.core.windows.net",
    "tO25mDhGaG8y2cvavLme0Bf3j1wBDaPF9Svhu7wWjVsBk2WQF0LTeDp2TNbx7zVWwUME2x6E009H+AStw51SDQ=="
)

# Parametros recibidos desde ADF
raw_path   = dbutils.widgets.get("rawPath")
silver_path = dbutils.widgets.get("silverPath")

# 1) Leer CSV raw
df = (spark.read
      .option("header", True)
      .option("inferSchema", False)  # ya sabemos tipos
      .schema("id_cliente INT, fecha_consumo STRING, consumo_kwh DOUBLE, tipo_servicio STRING, region STRING, estado_medidor STRING")
      .csv(raw_path))

# 2) Cast fecha_consumo a timestamp
df = df.withColumn("fecha_consumo_ts",
                   F.to_timestamp("fecha_consumo", "yyyy-MM-dd HH:mm:ss"))

# 3) Normalizar tipo_servicio a minúsculas
df = df.withColumn("tipo_servicio_norm", F.lower(F.col("tipo_servicio")))

# 4) Eliminar nulos en consumo_kwh
df = df.filter(F.col("consumo_kwh").isNotNull())

# 5) Añadir categoria_consumo
df = df.withColumn(
    "categoria_consumo",
    F.when(F.col("consumo_kwh") > 10, "Alto")
     .when(F.col("consumo_kwh") >= 5, "Medio")
     .otherwise("Bajo")
)

# 6) Guardar en Parquet en silver
(df.write
   .mode("overwrite")
   .parquet(silver_path))

# 7) Verificar completitud
src_count = df.count()
tgt_count = (spark.read.parquet(silver_path).count())
if src_count != tgt_count:
    raise Exception(f"Mismatch rows: source={src_count} vs target={tgt_count}")
dbutils.notebook.exit(f"Transform OK: rows={src_count}")