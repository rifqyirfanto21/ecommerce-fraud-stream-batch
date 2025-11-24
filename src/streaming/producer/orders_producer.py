from kafka import KafkaProducer
import json
import pandas as pd
from src.generator.batch_generator import generate_users, generate_products
from src.generator.stream_generator import continuous_orders_stream

KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(order: dict):
    producer.send("orders", order)

if __name__ == "__main__":
    users = generate_users(500)
    products = generate_products(300)

    users_df = pd.DataFrame(users)
    products_df = pd.DataFrame(products)

    continuous_orders_stream(
        users_df,
        products_df,
        send_func=send_to_kafka,
        sleep_range=(0.3, 1.5)
    )