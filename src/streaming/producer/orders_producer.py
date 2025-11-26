from kafka import KafkaProducer
import json
from src.generator.stream_generator import continuous_orders_stream

KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(order: dict):
    producer.send("orders", order)

if __name__ == "__main__":
    continuous_orders_stream(
        send_func=send_to_kafka,
        sleep_range=(0.3, 1.5)
    )