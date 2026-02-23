# Databricks notebook source
from pyspark.sql import window as W
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

# 1. Configuración de Entorno
silver_table = "proyecto_divisas.silver.divisas_limpias"
gold_table = "proyecto_divisas.gold.analisis_divisas"

# Leemos la capa Silver (Nuestra Verdad Maestra)
df_silver = spark.read.table(silver_table)

# 2. Configuración de Ventanas (Window Specifications)
# Ventana base por par de divisas ordenada cronológicamente
window_spec = W.Window.partitionBy("currency_pair").orderBy("event_timestamp")

# Ventana para promedios móviles y desviaciones (7 y 30 registros/días)
window_7d = window_spec.rowsBetween(-6, 0)
window_30d = window_spec.rowsBetween(-29, 0)

# Ventana diaria para máximos y mínimos (Partición por día)
window_day = W.Window.partitionBy("currency_pair", F.to_date("event_timestamp"))

# 3. Transformaciones y Cálculos Avanzados
df_gold = df_silver \
    .withColumn("prev_close", F.lag("price").over(window_spec)) \
    .withColumn("daily_return", 
                ((F.col("price") - F.col("prev_close")) / F.when(F.col("prev_close") == 0, None).otherwise(F.col("prev_close"))) * 100) \
    .withColumn("sma_7", F.avg("price").over(window_7d)) \
    .withColumn("sma_30", F.avg("price").over(window_30d)) \
    .withColumn("stddev_30", F.stddev("price").over(window_30d)) \
    .withColumn("bollinger_upper", F.col("sma_30") + (F.col("stddev_30") * 2)) \
    .withColumn("bollinger_lower", F.col("sma_30") - (F.col("stddev_30") * 2)) \
    .withColumn("max_24h", F.max("price").over(window_day)) \
    .withColumn("min_24h", F.min("price").over(window_day)) \
    .withColumn("trend_signal", 
                F.when(F.col("price") > F.col("sma_30"), "Bullish")
                .when(F.col("price") < F.col("sma_30"), "Bearish")
                .otherwise("Neutral"))

# 4. Formateo Final y Selección de Columnas para el Dashboard
# Aplicamos cast a Decimal para optimizar almacenamiento y precisión financiera
df_final = df_gold.select(
    "event_timestamp",
    "currency_pair",
    F.col("price").cast(DecimalType(18, 4)),
    F.col("daily_return").cast(DecimalType(10, 2)).alias("daily_return_pct"),
    F.col("sma_7").cast(DecimalType(18, 4)),
    F.col("sma_30").cast(DecimalType(18, 4)),
    F.col("bollinger_upper").cast(DecimalType(18, 4)),
    F.col("bollinger_lower").cast(DecimalType(18, 4)),
    F.col("max_24h").cast(DecimalType(18, 4)),
    F.col("min_24h").cast(DecimalType(18, 4)),
    "trend_signal",
    F.current_timestamp().alias("processed_at")
)

# 5. Carga en Unity Catalog
# Nota: 'overwriteSchema' se usa para permitir la evolución de las columnas de Bollinger
(df_final.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(gold_table))

print(f"✅ Capa Gold finalizada exitosamente: {gold_table}")
