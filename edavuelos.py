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
# --- 3.4 Convertir a entero (opcional, para ML) ---
df_clean = df_clean.withColumn("retraso_aerolinea_min", col("retraso_aerolinea_min").cast(IntegerType()))
print("=== TRANSFORMACIONES COMPLETADAS ===")
df_clean.select("cod_aerolinea", "retraso_aerolinea_min", "estados_vuelo").show(20)

# ========================
# 4. ANÁLISIS EXPLORATORIO (EDA)
# ========================

# --- 4.1 Conteo total de vuelos por estado ---
print("=== CONTEO POR ESTADO DE VUELO ===")
status_count = df_clean.groupBy("estados_vuelo").count().orderBy(desc("count"))
status_count.show()

# --- 4.2 Estadísticas descriptivas de retrasos ---
print("=== ESTADÍSTICAS DE RETRASOS ===")
df_clean.select("retraso_aerolinea_min").describe().show()
# --- 4.3 Retraso promedio por aerolínea ---
print("=== RETRASO PROMEDIO POR AEROLÍNEA ===")
delay_by_airline = df_clean \
    .groupBy("cod_aerolinea") \
    .agg(
        round(avg("retraso_aerolinea_min"), 2).alias("AVG_RETRASO_MIN"),
        count("cod_aerolinea").alias("TOTAL_VUELOS")
    ) \
    .orderBy(desc("AVG_RETRASO_MIN"))

delay_by_airline.show(10)
# --- 4.4 Aeropuertos con más retrasos (TOP 10 origen) ---
print("=== AEROPUERTOS CON MÁS RETRASOS ===")
top_delayed_airports = df_clean \
    .filter(col("estados_vuelo") == "RETRASADO") \
    .groupBy("airport") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)
top_delayed_airports.show()
# ========================
# 5. ALMACENAR RESULTADOS EN PARQUET (OPTIMIZADO)
# ========================

df_clean.write.mode("overwrite").partitionBy("estados_vuelo").parquet("resultados/estados_vuelos")
print("=== RESULTADOS GUARDADOS EN PARQUET (PARTICIONADO POR ESTADO) ===")
spark.stop()
print("=== PROCESO COMPLETADO CON ÉXITO ===")

