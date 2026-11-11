"""Consumidor Kafka que grafica en tiempo real la telemetria de la estacion meteorologica.

Soporta de forma automatica y transparente tanto:
- Mensajes binarios compactos de 3 bytes (24 bits) [Seccion 3.4].
- Mensajes en formato JSON [Seccion 3.3].
"""
import argparse
import json
from collections import Counter
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Consumer, KafkaError

from codec import DIRECCIONES, decode_payload, inspeccionar_bits
from config import KAFKA_CONFIG, TOPIC

ANGULOS = {d: np.deg2rad(i * 45) for i, d in enumerate(DIRECCIONES)}

all_time, all_temp, all_hume, all_wind, all_modes = [], [], [], [], []


def procesar_mensaje(mensaje):
    """Detecta si el mensaje es binario (3 bytes) o JSON y lo decodifica."""
    raw = mensaje.value()
    tam = len(raw)

    if tam == 3:
        data = decode_payload(raw)
        modo_str = f"Binario (3 bytes / 24 bits: 0x{raw.hex().upper()})"
        return data, modo_str, "binario"

    try:
        data = json.loads(raw.decode("utf-8"))
        modo_str = f"JSON ({tam} bytes)"
        return data, modo_str, "json"
    except Exception:
        # Intento de rescate si son bytes
        data = decode_payload(raw)
        return data, f"Binario ({tam} bytes)", "binario"


def crear_figura(headless=False):
    if not headless:
        plt.ion()
    fig = plt.figure(figsize=(14, 8))
    fig.canvas.manager.set_window_title(f"Estacion Meteorologica IoT - Topic {TOPIC}")
    ax_temp = fig.add_subplot(2, 2, 1)
    ax_hume = fig.add_subplot(2, 2, 3)
    ax_wind = fig.add_subplot(1, 2, 2, projection="polar")
    return fig, ax_temp, ax_hume, ax_wind


def graficar(fig, ax_temp, ax_hume, ax_wind):
    ax_temp.clear()
    ax_temp.plot(all_time, all_temp, marker="o", color="tab:red", linewidth=1.5, markersize=5)
    ax_temp.set_title("Temperatura (°C)", fontsize=11, fontweight="bold")
    ax_temp.set_ylim(0, 110)
    ax_temp.grid(alpha=0.3)
    if all_temp:
        ax_temp.annotate(
            f"{all_temp[-1]:.2f} °C",
            (all_time[-1], all_temp[-1]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontweight="bold",
            color="darkred",
        )

    ax_hume.clear()
    ax_hume.plot(all_time, all_hume, marker="o", color="tab:blue", linewidth=1.5, markersize=5)
    ax_hume.set_title("Humedad Relativa (%)", fontsize=11, fontweight="bold")
    ax_hume.set_ylim(0, 100)
    ax_hume.set_xlabel("Hora de recepcion")
    ax_hume.grid(alpha=0.3)
    if all_hume:
        ax_hume.annotate(
            f"{all_hume[-1]} %",
            (all_time[-1], all_hume[-1]),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontweight="bold",
            color="navy",
        )

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
    ax_wind.set_title("Direccion del viento (frecuencia)\nNaranja: ultima lectura", pad=20, fontweight="bold")

    ultimo_txt = "Esperando datos..."
    ultimo_modo = all_modes[-1] if all_modes else "N/A"
    if all_temp:
        ultimo_txt = (
            f"Ultimo dato: {all_temp[-1]:.2f} °C | {all_hume[-1]} % | "
            f"Viento: {all_wind[-1]} | Formato: {ultimo_modo}"
        )
    fig.suptitle(
        f"Estacion Meteorologica IoT | Topic {TOPIC} | Mensajes recibidos: {len(all_temp)}\n{ultimo_txt}",
        fontsize=12,
        fontweight="bold",
    )
    fig.tight_layout()
    fig.canvas.draw_idle()


def main():
    parser = argparse.ArgumentParser(description="Consumidor de la estacion meteorologica")
    parser.add_argument("--group", default=f"grupo-{TOPIC}", help="group.id del consumidor")
    parser.add_argument("--save", help="Ruta de un PNG que se actualiza con cada dato nuevo")
    parser.add_argument("--headless", action="store_true", help="Ejecutar sin ventana grafica (guardar directo a PNG)")
    parser.add_argument("--exit-on-idle", type=float, default=0, help="Segundos de inactividad antes de salir (0 = nunca)")
    args = parser.parse_args()

    if args.headless:
        import matplotlib
        matplotlib.use("Agg")

    config = dict(KAFKA_CONFIG)
    config.update({
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])

    fig, ax_temp, ax_hume, ax_wind = crear_figura(headless=args.headless)
    graficar(fig, ax_temp, ax_hume, ax_wind)

    print("=" * 60)
    print(f"Escuchando topic '{TOPIC}' | Grupo: '{args.group}'")
    print("Soporte activo para JSON y Binario (3 bytes). Ctrl+C para detener.")
    print("=" * 60)

    idle_start = None

    try:
        while args.headless or plt.fignum_exists(fig.number):
            mensaje = consumer.poll(0.5)
            if mensaje is None:
                if not args.headless:
                    plt.pause(0.1)
                if args.exit_on_idle > 0:
                    if idle_start is None:
                        idle_start = datetime.now()
                    elif (datetime.now() - idle_start).total_seconds() >= args.exit_on_idle:
                        print(f"Inactividad de {args.exit_on_idle}s alcanzada. Saliendo.")
                        break
                continue

            idle_start = None

            if mensaje.error():
                if mensaje.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] Kafka: {mensaje.error()}")
                continue

            try:
                payload, modo_str, tipo = procesar_mensaje(mensaje)
                all_temp.append(float(payload["temperatura"]))
                all_hume.append(int(payload["humedad"]))
                all_wind.append(payload["direccion_viento"])
                all_modes.append(modo_str)
            except Exception as e:
                print(f"[AVISO] Mensaje ignorado ({e}): {mensaje.value()!r}")
                continue

            _, ts = mensaje.timestamp()
            if ts and ts > 0:
                dt = datetime.fromtimestamp(ts / 1000)
            else:
                dt = datetime.now()
            all_time.append(dt)

            print(
                f"[Recibido] Offset: {mensaje.offset()} | Modo: {modo_str} | "
                f"Data: {payload}"
            )
            if tipo == "binario":
                print(f"           Detalle bits: {inspeccionar_bits(mensaje.value())}")

            graficar(fig, ax_temp, ax_hume, ax_wind)
            if args.save:
                fig.savefig(args.save, dpi=200)

            if not args.headless:
                plt.pause(0.1)

    except KeyboardInterrupt:
        print("\nConsumidor detenido.")
    finally:
        if args.save and all_temp:
            fig.savefig(args.save, dpi=200)
            print(f"Grafica guardada exitosamente en: {args.save}")
        consumer.close()


if __name__ == "__main__":
    main()
