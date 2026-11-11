"""Productor Kafka que simula un nodo de sensores de una estacion meteorologica."""
import argparse
import json
import random
import time

from confluent_kafka import Producer

from config import KAFKA_CONFIG, TOPIC

DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
MEDIA = 55
DESVIACION = 15


def generar_temperatura():
    valor = random.gauss(MEDIA, DESVIACION)
    return round(min(max(valor, 0.0), 110.0), 2)


def generar_humedad():
    valor = round(random.gauss(MEDIA, DESVIACION))
    return int(min(max(valor, 0), 100))


def generar_direccion():
    return random.choice(DIRECCIONES)


def generar_data():
    return {
        "temperatura": generar_temperatura(),
        "humedad": generar_humedad(),
        "direccion_viento": generar_direccion(),
    }


def reporte_entrega(err, msg):
    if err is not None:
        print(f"Error al enviar: {err}")
    else:
        print(f"Enviado a {msg.topic()} [particion {msg.partition()}] offset {msg.offset()}")


def main():
    parser = argparse.ArgumentParser(description="Productor de la estacion meteorologica")
    parser.add_argument("--min", type=float, default=15, help="Segundos minimos entre envios")
    parser.add_argument("--max", type=float, default=30, help="Segundos maximos entre envios")
    args = parser.parse_args()

    producer = Producer(KAFKA_CONFIG)
    print(f"Enviando datos al topic {TOPIC}. Ctrl+C para detener.")

    try:
        while True:
            data = generar_data()
            payload = json.dumps(data)
            print(f"Generado: {payload}")
            producer.produce(TOPIC, key="sensor1", value=payload.encode("utf-8"), callback=reporte_entrega)
            producer.poll(0)
            producer.flush()
            time.sleep(random.uniform(args.min, args.max))
    except KeyboardInterrupt:
        print("Productor detenido.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
