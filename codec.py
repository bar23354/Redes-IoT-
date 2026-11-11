"""Modulo de codificacion y decodificacion compacta de 3 bytes (24 bits)

Permite enviar telemetria meteorologica respetando la restriccion de
tamano maximo de carga util (payload <= 3 bytes / 24 bits) para redes IoT
de bajo consumo (LoRa, Sigfox).

Distribucion de los 24 bits:
- Temperatura: [0, 110.00] °C con 2 decimales -> factor x100 (0 a 11000) -> 14 bits (0 a 16383)
- Humedad relativa: [0, 100] % entero -> 7 bits (0 a 127)
- Direccion del viento: 8 direcciones cardinales/intercardinales -> 3 bits (0 a 7)
Total: 14 + 7 + 3 = 24 bits = 3 bytes exactos.
"""

import json

DIRECCIONES = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
DIR_A_INDICE = {d: i for i, d in enumerate(DIRECCIONES)}


def encode_payload(data: dict) -> bytes:
    """Codifica un diccionario con telemetria en exactamente 3 bytes (24 bits).

    data = {
        "temperatura": float [0.0, 110.00],
        "humedad": int [0, 100],
        "direccion_viento": str in DIRECCIONES
    }
    """
    temp = float(data["temperatura"])
    temp = max(0.0, min(110.0, temp))
    temp_int = int(round(temp * 100)) & 0x3FFF  # 14 bits

    hum = int(data["humedad"])
    hum = max(0, min(100, hum))
    hum_int = hum & 0x7F  # 7 bits

    viento_str = str(data["direccion_viento"]).strip().upper()
    viento_int = DIR_A_INDICE.get(viento_str, 0) & 0x07  # 3 bits

    # Empaquetado: [14 bits Temp][7 bits Hum][3 bits Viento]
    valor_24bit = (temp_int << 10) | (hum_int << 3) | viento_int
    return valor_24bit.to_bytes(3, byteorder="big")


def decode_payload(raw_bytes: bytes) -> dict:
    """Decodifica un arreglo de 3 bytes en un diccionario con la telemetria."""
    if len(raw_bytes) != 3:
        raise ValueError(f"Se esperaban 3 bytes, pero se recibieron {len(raw_bytes)} bytes")

    valor_24bit = int.from_bytes(raw_bytes, byteorder="big")

    temp_int = (valor_24bit >> 10) & 0x3FFF
    hum_int = (valor_24bit >> 3) & 0x7F
    viento_int = valor_24bit & 0x07

    temperatura = round(temp_int / 100.0, 2)
    humedad = hum_int
    direccion = DIRECCIONES[viento_int]

    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion,
    }


def json_to_bytes(json_str: str) -> bytes:
    """Encode (JSON to Bytes): Transforma una cadena JSON a 3 bytes."""
    data = json.loads(json_str)
    return encode_payload(data)


def bytes_to_json(raw_bytes: bytes) -> str:
    """Decode (Bytes to JSON): Transforma 3 bytes a una cadena JSON."""
    data = decode_payload(raw_bytes)
    return json.dumps(data)


def inspeccionar_bits(raw_bytes: bytes) -> str:
    """Retorna una representacion visual legible de los 24 bits."""
    if len(raw_bytes) != 3:
        return f"{len(raw_bytes)} bytes (no es payload de 24 bits)"
    val = int.from_bytes(raw_bytes, byteorder="big")
    bits = f"{val:024b}"
    b_temp = bits[0:14]
    b_hum = bits[14:21]
    b_wind = bits[21:24]
    return f"[{b_temp} (Temp: 14b) | {b_hum} (Hum: 7b) | {b_wind} (Viento: 3b)] = {len(raw_bytes)} bytes"
