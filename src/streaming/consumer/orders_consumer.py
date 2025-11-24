from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    print("Received:", msg.value)