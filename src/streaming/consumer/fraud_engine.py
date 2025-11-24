from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timezone

KAFKA_BROKER = "localhost:9092"  # or "kafka:9092" if running inside Docker

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer_processed = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer_fraud = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# === Fraud rules ===
def apply_fraud_rules(order: dict) -> str:
    """Return 'Fraud' or 'Genuine' based on the rules."""
    # Rule A: Country not ID
    if order.get("country") != "ID":
        return "Fraud"

    created_dt = datetime.fromisoformat(order["created_date"])
    hour = created_dt.hour

    # Rule B: Quantity anomaly in midnight UTC window
    if 0 <= hour < 4 and order.get("quantity", 0) > 100:
        return "Fraud"

    # Rule C: Amount anomaly in midnight UTC window
    if 0 <= hour < 4 and order.get("amount_numeric", 0) > 100_000_000:
        return "Fraud"

    # Rule D: Botting behavior
    if order.get("source") == "streaming" and order.get("bot_flag", False):
        return "Fraud"

    return "Genuine"

print("Fraud engine started... Listening to orders topic.")
try:
    for msg in consumer:
        order = msg.value

        status = apply_fraud_rules(order)
        order["status"] = status

        producer_processed.send("processed_orders", order)

        if status == "Fraud":
            producer_fraud.send("fraud_alerts", order)

except KeyboardInterrupt:
    print("Stopping fraud engine...")

finally:
    consumer.close()
    producer_processed.close()
    producer_fraud.close()