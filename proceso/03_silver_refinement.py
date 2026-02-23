# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import DecimalType

# 1. Configuración de tablas
bronze_hist_table = "proyecto_divisas.bronze.historico_divisas"
bronze_api_table = "proyecto_divisas.bronze.api_divisas_actual"
silver_table = "proyecto_divisas.silver.divisas_limpias"

# 2. Lectura de fuentes Bronze
df_hist = spark.read.table(bronze_hist_table)
df_api = spark.read.table(bronze_api_table)

# 3. Estandarización de Esquemas
# Corregimos: 'Series' -> 'Symbol' y 'Price' -> 'Close'
df_hist_aligned = df_hist.select(
    col("Date").alias("event_timestamp"),
    col("Symbol").alias("currency_pair"), # 'Symbol' contiene "COP=X", "MXN=X", etc.
    col("Close").alias("price")           # Usamos el precio de cierre
)

# Alineamos la API (asumiendo que tiene estos nombres)
df_api_aligned = df_api.select(
    col("event_timestamp"),
    col("currency_pair"),
    col("price")
)

# Unión de ambas fuentes
df_union = df_hist_aligned.unionByName(df_api_aligned)

# 4. Limpieza y Tipado (Senior Practice)
df_cleaned = df_union \
    .filter(col("price").isNotNull() & col("event_timestamp").isNotNull()) \
    .withColumn("price", col("price").cast(DecimalType(18, 4))) \
    .withColumn("event_date", to_date(col("event_timestamp"))) \
    .dropDuplicates(["event_timestamp", "currency_pair"])

# 5. Carga en Capa Silver
(df_cleaned.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(silver_table))

print(f"✅ Capa Silver optimizada en {silver_table}")
