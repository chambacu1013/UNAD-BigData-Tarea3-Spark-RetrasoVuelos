import time 
import json 
import random 
from kafka import KafkaProducer
import csv

# Configuración
bootstrap_servers = ['localhost:9092']
topic_name = 'flight_delays'

# Crear productor
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ruta al CSV (ajusta si es necesario)
csv_path = "/home/vboxuser/data/flight-data-2024.csv"

print("Iniciando productor Kafka... enviando vuelos cada 1 segundo")
with open(csv_path, 'r') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        # Simular llegada en tiempo real
        time.sleep(1)  # 1 vuelo por segundo
        
        # Enviar solo columnas clave
        flight_data = {
            "FL_DATE": row.get("FL_DATE"),
            "AIRLINE": row.get("CARRIER"),
            "ORIGIN": row.get("ORIGIN"),
            "DEST": row.get("DEST"),
            "ARR_DELAY": row.get("ARR_DELAY")
        }
        producer.send(topic_name, value=flight_data)
        print(f"Enviado: {flight_data['AIRLINE']} - {flight_data['ORIGIN']} → {flight_data['DEST']} (Retraso: {flight_data['ARR_DELAY']} min)")
        
        if i >= 100:  # Enviar solo 100 para demo
            break

producer.flush()
producer.close()
print("Productor finalizado.")
