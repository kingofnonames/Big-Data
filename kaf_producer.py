from kafka import KafkaProducer
import json
from config import KAFKA_BROKER

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False).encode("utf-8")
)


def send_to_kafka(topic: str, message: dict):
    producer.send(topic, message)
    producer.flush()
