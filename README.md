# UNAD-BigData-Tarea3-Spark-RetrasoVuelos
## Procesamiento batch y streaming de datos de vuelos con Spark, Hadoop y Kafka - Tarea 3 UNAD

> **Curso**: Big Data (202016911)  
> **Resultado de Aprendizaje 3**: Diseñar e implementar soluciones de almacenamiento y procesamiento de grandes volúmenes de datos utilizando Hadoop, Spark y Kafka.  
> **Entrega**: 28 de octubre de 2025

---

## Descripción del Problema
**Predicción y análisis de retrasos en vuelos** para mejorar la gestión operativa de aerolíneas y aeropuertos mediante:
- Procesamiento **batch** con Spark (EDA, limpieza, almacenamiento en Parquet)
- Procesamiento **en tiempo real** con Spark Streaming + Kafka

**Dataset**: `flight-data-2024.csv` (~7M registros) - [Fuente: Kaggle](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)

## Arquitectura Implementada

[CSV en HDFS] → [Spark Batch] → [EDA + Parquet Particionado]
↓
[Kafka Topic] → [Spark Streaming] → [Alertas en tiempo real]

---

## Requisitos
- Ubuntu Server (VM)
- Hadoop 3.3.6 (HDFS)
- Apache Spark 3.5+
- Apache Kafka 3.7+
- Python 3.5

---

## Estructura del Proyecto
src/batch/edavuelos.py               → Procesamiento batch
src/streaming/producer.py            → Simula datos en tiempo real
src/streaming/consumer_spark.py      → Spark Streaming

---

## Instrucciones de Ejecución

### 1. Configurar HDFS
```bash
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put data/Airline_Delay_Cause.csv /Tarea3/
### 2. Ejecutar Procesamiento Batch
python3 edavuelos.py
Salida:
EDA: retrasos por aerolínea, aeropuertos, estadísticas
Resultados en: parquet resultados/estados_vuelos
### 3. Iniciar Kafka
# Iniciar Zookeeper
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
# Iniciar kafka
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
### 4. Crear Topic
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
### 5. Ejecutar Streaming
python3 kafka_producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py

Autores

MICHAEL YESID MEDINA VILLAMIZAR
JUAN PABLO QUINTERO CASTELLANOS
RAFAEL JOSE JACOME MADARIAGA
GRUPO COLABORATIVO 29
