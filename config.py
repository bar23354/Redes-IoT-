import os

BOOTSTRAP_SERVER = os.getenv("KAFKA_SERVER", "161.35.11.141:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "23361")

KAFKA_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USER", "estudiante"),
    "sasl.password": os.getenv("KAFKA_PASSWORD", "TryluRandomPass-2!@!"),
}
