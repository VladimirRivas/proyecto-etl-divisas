# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

import yfinance as yf
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# 1. Configuración de Activos y Destino
# Usamos los tickers específicos de Yahoo Finance para los pares solicitados
tickers_mapping = {
    "COP=X": "USD/COP",
    "MXN=X": "USD/MXN",
    "EURUSD=X": "EUR/USD",
    "BRL=X": "BRL/USD",
    "BTC-USD": "BTC/USD"
}
target_table = "proyecto_divisas.bronze.api_divisas_actual"

# 2. Extracción (Corre en el Driver)
print(f"Descargando datos para: {list(tickers_mapping.keys())}")
data = yf.download(list(tickers_mapping.keys()), period="1d", interval="1m")

# 3. Transformación Básica en Pandas (Preparación para Spark)
# Obtenemos solo los precios de cierre y reseteamos el índice para tener la fecha/hora
df_pd = data['Close'].reset_index()

# Convertimos de formato ancho (columnas por ticker) a formato largo (filas por ticker)
df_melted = df_pd.melt(id_vars=['Datetime'], var_name='ticker_raw', value_name='price')

# 4. Conversión a Spark DataFrame y Enriquecimiento
df_spark = spark.createDataFrame(df_melted)

# Aplicamos el mapeo de nombres de activos y metadatos de auditoría
df_bronze_api = df_spark \
    .withColumn("currency_pair", col("ticker_raw")) \
        .replace(tickers_mapping, subset=["currency_pair"]) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("Yahoo Finance API")) \
    .select(
        col("Datetime").alias("event_timestamp"),
        "currency_pair",
        "price",
        "ingestion_timestamp",
        "source_system"
    )

# 5. Carga a Bronze (Append para mantener el histórico de llamadas a la API)
(df_bronze_api.write
 .format("delta")
 .mode("append") 
 .saveAsTable(target_table))

print(f"✅ Ingesta incremental completada exitosamente en {target_table}")
