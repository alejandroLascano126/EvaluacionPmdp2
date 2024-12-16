import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Define límites de coordenadas para Samborondón y Daule
COORD_LIMITS = {
    "lat_min": -1.9267,
    "lat_max": -1.8760,
    "lon_min": -79.9191,
    "lon_max": -79.8355
}

# Lista de ciudades/regiones
CIUDADES = ["Samborondón", "Daule"]

# Número de medidores simulados
NUM_MEDIDORES = 10

# Generar IDs de medidores
MEDIDORES = [f"MED-{i:03d}" for i in range(1, NUM_MEDIDORES + 1)]

# Configuración de Kafka
BROKER = "localhost:9092"
TOPICO_GENERAL = "consumo_electrico"
TOPICO_SAMBORONDON = "consumo_samborondon"
TOPICO_DAULE = "consumo_daule"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generar_datos():
    """Genera una lectura de consumo eléctrico simulada."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    consumo = round(random.uniform(0.1, 5.0), 2)  # Consumo en kWh
    latitud = round(random.uniform(COORD_LIMITS["lat_min"], COORD_LIMITS["lat_max"]), 6)
    longitud = round(random.uniform(COORD_LIMITS["lon_min"], COORD_LIMITS["lon_max"]), 6)
    id_medidor = random.choice(MEDIDORES)
    ciudad = random.choice(CIUDADES)

    return {
        "timestamp": timestamp,
        "consumo_kwh": consumo,
        "latitud": latitud,
        "longitud": longitud,
        "id_medidor": id_medidor,
        "ciudad": ciudad
    }

def enviar_a_kafka(datos):
    """Envía los datos simulados al tópico correspondiente en Kafka."""
    producer.send(TOPICO_GENERAL, value=datos)
    if datos["ciudad"] == "Samborondón":
        producer.send(TOPICO_SAMBORONDON, value=datos)
    elif datos["ciudad"] == "Daule":
        producer.send(TOPICO_DAULE, value=datos)

def main():
    """Simula generación continua de datos de consumo eléctrico."""
    try:
        print("Iniciando simulador de consumo eléctrico...")
        while True:
            datos = generar_datos()
            enviar_a_kafka(datos)
            print(f"Datos enviados a Kafka: {datos}")
            time.sleep(1)  # Frecuencia de 1 lectura por segundo
    except KeyboardInterrupt:
        print("\nSimulación detenida.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
