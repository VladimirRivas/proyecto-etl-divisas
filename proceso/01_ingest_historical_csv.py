# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# 1. Configuración de rutas
raw_path = "abfss://raw@stcurrencydatalakeprod.dfs.core.windows.net/historico_divisas.csv"
target_table = "proyecto_divisas.bronze.historico_divisas"

# 2. Lectura con Spark (Aprovechando metadatos de Unity Catalog)
df_raw_hist = (spark.read
               .format("csv")
               .option("header", "true")
               .option("inferSchema", "true")
               .option("delimiter", ",")
               .load(raw_path))

# 3. Añadir Metadatos (Usando la columna oculta _metadata)
# En Unity Catalog, '_metadata' contiene file_path, file_name, file_size, etc.
df_bronze_hist = df_raw_hist \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", df_raw_hist["_metadata.file_path"])

# 4. Escritura en Unity Catalog
(df_bronze_hist.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(target_table))

print(f"✅ Carga exitosa en {target_table}")
