"""Consumidor Kafka que grafica en tiempo real la telemetria de la estacion meteorologica."""
import argparse
import json
from collections import Counter
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Consumer, KafkaError

from config import KAFKA_CONFIG, TOPIC

DIRECCIONES = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
ANGULOS = {d: np.deg2rad(i * 45) for i, d in enumerate(DIRECCIONES)}

all_time, all_temp, all_hume, all_wind = [], [], [], []


def procesar_mensaje(mensaje):
    return json.loads(mensaje.value().decode("utf-8"))


def crear_figura():
    plt.ion()
    fig = plt.figure(figsize=(14, 8))
    fig.canvas.manager.set_window_title(f"Estacion Meteorologica IoT - Topic {TOPIC}")
    ax_temp = fig.add_subplot(2, 2, 1)
    ax_hume = fig.add_subplot(2, 2, 3)
    ax_wind = fig.add_subplot(1, 2, 2, projection="polar")
    return fig, ax_temp, ax_hume, ax_wind


def graficar(fig, ax_temp, ax_hume, ax_wind):
    ax_temp.clear()
    ax_temp.plot(all_time, all_temp, marker="o", color="tab:red")
    ax_temp.set_title("Temperatura (°C)")
    ax_temp.set_ylim(0, 110)
    ax_temp.grid(alpha=0.3)
    if all_temp:
        ax_temp.annotate(f"{all_temp[-1]:.2f} °C", (all_time[-1], all_temp[-1]),
                         textcoords="offset points", xytext=(0, 8), ha="center")

    ax_hume.clear()
    ax_hume.plot(all_time, all_hume, marker="o", color="tab:blue")
    ax_hume.set_title("Humedad relativa (%)")
    ax_hume.set_ylim(0, 100)
    ax_hume.set_xlabel("Hora de recepcion")
    ax_hume.grid(alpha=0.3)
    if all_hume:
        ax_hume.annotate(f"{all_hume[-1]} %", (all_time[-1], all_hume[-1]),
                         textcoords="offset points", xytext=(0, 8), ha="center")

    for ax in (ax_temp, ax_hume):
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        ax.tick_params(axis="x", rotation=30)

    ax_wind.clear()
    conteo = Counter(all_wind)
    angulos = [ANGULOS[d] for d in DIRECCIONES]
    valores = [conteo.get(d, 0) for d in DIRECCIONES]
    ax_wind.bar(angulos, valores, width=np.deg2rad(40), color="tab:green", alpha=0.7)
    if all_wind:
        ultimo = all_wind[-1]
        ax_wind.bar(ANGULOS[ultimo], conteo[ultimo], width=np.deg2rad(40), color="tab:orange")
    ax_wind.set_theta_zero_location("N")
    ax_wind.set_theta_direction(-1)
    ax_wind.set_xticks(angulos)
    ax_wind.set_xticklabels(DIRECCIONES)
    ax_wind.set_title("Direccion del viento (frecuencia)\nNaranja: ultima lectura", pad=20)

    ultimo_txt = ""
    if all_temp:
        ultimo_txt = (f"Ultimo dato: {all_temp[-1]:.2f} °C, {all_hume[-1]} %, "
                      f"viento {all_wind[-1]}")
    fig.suptitle(f"Estacion Meteorologica IoT | Topic {TOPIC} | Mensajes: {len(all_temp)}\n{ultimo_txt}")
    fig.tight_layout()
    fig.canvas.draw_idle()


def main():
    parser = argparse.ArgumentParser(description="Consumidor de la estacion meteorologica")
    parser.add_argument("--group", default=f"grupo-{TOPIC}", help="group.id del consumidor")
    parser.add_argument("--save", help="Ruta de un PNG que se actualiza con cada dato nuevo")
    args = parser.parse_args()

    config = dict(KAFKA_CONFIG)
    config.update({"group.id": args.group, "auto.offset.reset": "earliest"})
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    fig, ax_temp, ax_hume, ax_wind = crear_figura()
    graficar(fig, ax_temp, ax_hume, ax_wind)
    print(f"Escuchando el topic {TOPIC}. Ctrl+C o cerrar la ventana para detener.")

    try:
        while plt.fignum_exists(fig.number):
            mensaje = consumer.poll(0.5)
            if mensaje is None:
                plt.pause(0.1)
                continue
            if mensaje.error():
                if mensaje.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {mensaje.error()}")
                continue

            try:
                payload = procesar_mensaje(mensaje)
                all_temp.append(float(payload["temperatura"]))
                all_hume.append(int(payload["humedad"]))
                all_wind.append(payload["direccion_viento"])
            except (ValueError, KeyError, UnicodeDecodeError) as e:
                print(f"Mensaje ignorado ({e}): {mensaje.value()}")
                continue

            _, ts = mensaje.timestamp()
            all_time.append(datetime.fromtimestamp(ts / 1000))
            print(f"Recibido offset {mensaje.offset()}: {payload}")

            graficar(fig, ax_temp, ax_hume, ax_wind)
            if args.save:
                fig.savefig(args.save, dpi=200)
            plt.pause(0.1)
    except KeyboardInterrupt:
        print("Consumidor detenido.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
