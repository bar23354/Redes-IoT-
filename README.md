# Estación Meteorológica IoT con Apache Kafka

Universidad del Valle de Guatemala, CC3067 Redes.

- Roberto Barreda, 23354
- Javier España, 23361

Topic utilizado: `23361`.

## Descripción

`producer.py` simula un nodo con tres sensores y publica una lectura en formato JSON cada 15 a 30 segundos en el broker Kafka (`161.35.11.141:9092`), con key `sensor1`. Corre hasta que se interrumpe con Ctrl+C.

- Temperatura: distribución normal con media 55 y desviación estándar 15, limitada a [0, 110] °C, dos decimales.
- Humedad relativa: distribución normal con media 55 y desviación estándar 15, limitada a [0, 100] %, entero.
- Dirección del viento: distribución uniforme sobre {N, NO, O, SO, S, SE, E, NE}.

Ejemplo de mensaje:

```json
{"temperatura": 56.32, "humedad": 51, "direccion_viento": "SO"}
```

`consumer.py` se suscribe al mismo topic, lee los mensajes desde el inicio (`auto.offset.reset=earliest`) y actualiza la gráfica cada vez que entra un dato nuevo:

- Temperatura en el tiempo.
- Humedad relativa en el tiempo.
- Rosa de vientos con la frecuencia de cada dirección. La última lectura se marca en naranja.

`config.py` contiene el servidor, el topic y los parámetros de autenticación SASL_PLAINTEXT / PLAIN. Se pueden cambiar con las variables de entorno `KAFKA_SERVER`, `KAFKA_TOPIC`, `KAFKA_USER` y `KAFKA_PASSWORD`.

## Ejecución

```bash
pip install -r requirements.txt
```

En una terminal:

```bash
python producer.py
```

En otra terminal:

```bash
python consumer.py
```

Opciones:

- `python producer.py --min 2 --max 4` reduce el intervalo entre envíos para pruebas.
- `python consumer.py --save grafica.png` guarda la gráfica en un PNG con cada dato nuevo.
- `python consumer.py --group otro-grupo` usa otro `group.id`. Con un grupo nuevo se vuelve a leer todo el historial del topic.

## Captura

`capturas/prueba_broker_local.png` es una prueba hecha contra un broker Kafka local con la misma configuración SASL. La captura de entrega se genera contra el servidor del curso y se guarda como `capturas/grafica_consumidor.png`:

```bash
python consumer.py --save capturas/grafica_consumidor.png
```
