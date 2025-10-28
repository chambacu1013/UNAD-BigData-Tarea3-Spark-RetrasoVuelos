from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col, window 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType 
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO 
spark = SparkSession.builder \ 
  .appName("KafkaSparkStreaming") \ 
  .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema del mensaje JSON
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("AIRLINE", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("DEST", StringType()),
    StructField("ARR_DELAY", StringType())
])

# Leer stream desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight_delays") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir valor (bytes) a string y luego a JSON
df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Convertir ARR_DELAY a entero
df = df.withColumn("ARR_DELAY", col("ARR_DELAY").cast(IntegerType()))

# Filtrar retrasos > 15 min
delayed_flights = df.filter(col("ARR_DELAY") > 15)

# Mostrar alertas en consola
query = delayed_flights \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("Spark Streaming iniciado... esperando vuelos retrasados >15 min")
query.awaitTermination()
