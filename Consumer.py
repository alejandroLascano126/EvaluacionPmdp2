import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
from hdfs import InsecureClient  # Librería para interactuar con HDFS

# Configuración de Kafka
BROKER = "localhost:9092"
TOPICO_GENERAL = "consumo_electrico"
TOPICO_SAMBORONDON = "consumo_samborondon"
TOPICO_DAULE = "consumo_daule"

# Configuración de HDFS
HDFS_NAMENODE = 'http://localhost:9870'  # Dirección del Namenode
HDFS_DIR = '/user/hadoop/subidaDeEvaluacion/'  # Ruta de la carpeta que has creado en HDFS

# Crear un cliente de HDFS
client = InsecureClient(HDFS_NAMENODE)

# Crear un consumidor de Kafka
consumer = KafkaConsumer(
    TOPICO_GENERAL,
    TOPICO_SAMBORONDON,
    TOPICO_DAULE,
    bootstrap_servers=[BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="consumo_group"
)

# Ventana de tiempo de 1 minuto
VENTANA_TIEMPO = timedelta(minutes=1)

# Para almacenar los datos agrupados por ventana de tiempo y región
ventanas_consumo = {}

def detectar_picos(consumo_data):
    """
    Detecta picos de consumo basados en la desviación estándar.
    Un pico se considera si el consumo es mayor que 2 desviaciones estándar de la media.
    """
    consumos = [d['consumo_kwh'] for d in consumo_data]

    if len(consumos) < 2:
        return []

    # Calcular la media y desviación estándar
    media = np.mean(consumos)
    desviacion_estandar = np.std(consumos)

    # Definir el umbral para picos
    umbral_superior = media + 2 * desviacion_estandar
    umbral_inferior = media - 2 * desviacion_estandar

    # Detectar los valores que están fuera de los umbrales
    picos = [d for d in consumo_data if d['consumo_kwh'] > umbral_superior or d['consumo_kwh'] < umbral_inferior]
    return picos

def calcular_consumo_promedio(consumo_data):
    """
    Calcula el consumo promedio de electricidad dentro de una ventana de tiempo.
    """
    if len(consumo_data) == 0:
        return 0

    consumos = [d['consumo_kwh'] for d in consumo_data]
    promedio = np.mean(consumos)
    return promedio

def guardar_datos_en_hdfs(ventana, region, promedio, picos):
    """
    Guarda los resultados de cada ventana de tiempo y región en HDFS.
    """
    # Convertir la ventana a un formato adecuado para el nombre del archivo
    ventana_str = ventana.strftime("%Y-%m-%d_%H-%M-%S")  # Formato sin los dos puntos
    archivo = f"consumo_{ventana_str}_{region}.txt"
    ruta = HDFS_DIR + archivo

    # Preparar el contenido a escribir
    contenido = f"Ventana: {ventana} | Región: {region} | Promedio Consumo: {promedio:.2f} kWh\n"
    if picos:
        contenido += f"Picos detectados: {picos}\n"
    else:
        contenido += "No se detectaron picos.\n"

    # Subir los datos a HDFS
    with client.write(ruta, overwrite=True) as writer:
        writer.write(contenido)
        print(f"Datos guardados en: {ruta}")


def procesar_datos(consumer):
    """
    Consume los mensajes de Kafka, los agrupa por ventanas de tiempo y región, y detecta los picos de consumo eléctrico.
    """
    consumo_data = {}  # Diccionario para almacenar los datos por región y ventana de tiempo

    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S")
            region = data['ciudad']

            # Definir la clave de la ventana de tiempo (minuto)
            ventana_clave = timestamp.replace(second=0, microsecond=0)

            if ventana_clave not in consumo_data:
                consumo_data[ventana_clave] = {}

            if region not in consumo_data[ventana_clave]:
                consumo_data[ventana_clave][region] = []

            # Añadir el dato a la ventana y región correspondiente
            consumo_data[ventana_clave][region].append(data)

            # Calcular el promedio y detectar picos por cada región en cada ventana
            for ventana, regiones in consumo_data.items():
                for region, datos_region in regiones.items():
                    promedio = calcular_consumo_promedio(datos_region)
                    picos = detectar_picos(datos_region)

                    # Mostrar el consumo promedio y los picos detectados
                    print(f"Ventana: {ventana} | Región: {region} | Promedio Consumo: {promedio:.2f} kWh")

                    if picos:
                        print(f"  Picos detectados en {region}: {picos}")
                    else:
                        print(f"  No se detectaron picos en {region}.")

                    # Guardar los resultados en HDFS
                    guardar_datos_en_hdfs(ventana, region, promedio, picos)

    except KeyboardInterrupt:
        print("\nConsumo detenido.")
    finally:
        consumer.close()

if __name__ == "__main__":
    procesar_datos(consumer)
