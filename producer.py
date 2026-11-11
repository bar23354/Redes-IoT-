"""Productor Kafka que simula un nodo de sensores de una estacion meteorologica.

Soporta dos modos de envio:
1. 'binary': Codificacion compacta en 3 bytes (24 bits) para redes IoT con
   restriccion de payload (Seccion 3.4).
2. 'json': Envio estandar en JSON (Seccion 3.2).
"""
import argparse
import json
import random
import time

from confluent_kafka import Producer

from codec import (
    DIRECCIONES,
    encode_payload,
    inspeccionar_bits,
)
from config import KAFKA_CONFIG, TOPIC

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
        print(f"[ERROR] Fallo al enviar mensaje: {err}")
    else:
        print(
            f"[OK] Enviado a {msg.topic()} [particion {msg.partition()}] "
            f"offset {msg.offset()} | tamano: {len(msg.value())} bytes"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Productor Kafka para Estacion Meteorologica IoT"
    )
    parser.add_argument(
        "--mode",
        choices=["binary", "json"],
        default="binary",
        help="Modo de empaquetado: 'binary' (3 bytes / 24 bits) o 'json' (estandar)",
    )
    parser.add_argument(
        "--min",
        type=float,
        default=15.0,
        help="Segundos minimos entre envios (defecto: 15)",
    )
    parser.add_argument(
        "--max",
        type=float,
        default=30.0,
        help="Segundos maximos entre envios (defecto: 30)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Numero de mensajes a enviar (0 para bucle infinito)",
    )
    args = parser.parse_args()

    producer = Producer(KAFKA_CONFIG)
    print("=" * 60)
    print(f"Iniciando Productor Kafka | Topic: {TOPIC} | Modo: {args.mode.upper()}")
    print(f"Intervalo de envio: [{args.min}, {args.max}] s")
    print("Presione Ctrl+C para detener.")
    print("=" * 60)

    sent = 0
    try:
        while True:
            data = generar_data()

            if args.mode == "binary":
                payload = encode_payload(data)
                chars_repr = "".join(chr(b) if 32 <= b <= 126 else f"\\x{b:02x}" for b in payload)
                print(f"\n[Dato #{sent + 1}] Generado: {data}")
                print(f"  Modo: BINARIO COMPACTO ({len(payload)} bytes / 24 bits)")
                print(f"  Hex: 0x{payload.hex().upper()} | Chars: '{chars_repr}'")
                print(f"  Bits: {inspeccionar_bits(payload)}")
            else:
                json_str = json.dumps(data)
                payload = json_str.encode("utf-8")
                print(f"\n[Dato #{sent + 1}] Generado: {data}")
                print(f"  Modo: JSON ({len(payload)} bytes)")
                print(f"  Payload: {json_str}")

            producer.produce(
                TOPIC,
                key="sensor1",
                value=payload,
                callback=reporte_entrega,
            )
            producer.poll(0)
            producer.flush()

            sent += 1
            if args.count > 0 and sent >= args.count:
                print(f"\nFinalizado envio de {sent} mensajes.")
                break

            espera = random.uniform(args.min, args.max)
            print(f"  Esperando {espera:.1f} s para siguiente medicion...")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
