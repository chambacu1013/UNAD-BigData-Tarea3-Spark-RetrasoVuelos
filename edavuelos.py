#Importamos librerias necesarias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, round, desc
from pyspark.sql.types import IntegerType
import sys
# ========================
# 1. INICIAR SESIÓN SPARK
# ========================
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
print("=== SESIÓN SPARK INICIADA ===")
# ========================
# 2. CARGAR DATOS DESDE HDFS
# ========================
file_path = 'hdfs://localhost:9000/Tarea3/Airline_Delay_Cause.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
# Vista rápida: Primeras 5 filas
print(f"Filas totales: {df.count():,}")
df.show(5, truncate=False)
df.printSchema()
# ========================
# 3. LIMPIEZA Y TRANSFORMACIÓN
# ========================

# --- 3.1 Eliminar filas con valores duplicados y nulos ---
df_clean = df.dropDuplicates().na.drop()
print(f"Filas tras eliminar nulos: {df_clean.count():,}")
# --- 3.2 Renombrar columnas para claridad ---
df_clean = df_clean.withColumnRenamed("carrier", "cod_aerolinea")
df_clean = df_clean.withColumnRenamed("arr_delay", "retraso_aerolinea_min")

# --- 3.3 Crear columna categórica: ESTADO DEL VUELO ---
# Retrasado si retraso_aerolinea_min > 15 minutos (estándar FAA)
df_clean= df_clean.withColumn(
    "estados_vuelo",
    when(col("retraso_aerolinea_min") > 15, "RETRASADO")
    .when(col("retraso_aerolinea_min") <= 0, "A TIEMPO")
    .otherwise("LIGERO RETRASO")
)
