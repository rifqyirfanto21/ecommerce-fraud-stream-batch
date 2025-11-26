from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta
from collections import deque

KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
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

BUFFER = deque(maxlen=50)
WINDOW = timedelta(seconds=10)

# BOT MODE A
# same user_id + same product_id + 3 consecutive orders
def detect_bot_mode_A(order):
    user = order["user_id"]
    pid = order["product_id"]
    now = datetime.fromisoformat(order["event_ts"])

    recent = [
        o for o in BUFFER
        if o["user_id"] == user
        and o["product_id"] == pid
        and now - datetime.fromisoformat(o["event_ts"]) < WINDOW
    ]

    return len(recent) >= 2   # + current = 3

# BOT MODE B:
# Sequential user_ids + same product + same qty
def detect_bot_mode_B(order):
    now = datetime.fromisoformat(order["event_ts"])

    recent = [
        o for o in BUFFER
        if now - datetime.fromisoformat(o["event_ts"]) < WINDOW
    ]

    if len(recent) < 2:
        return False

    # include current order at end
    seq = recent + [order]

    # extract values
    uids = [int(o["user_id"].replace("U", "")) for o in seq]
    pids = [o["product_id"] for o in seq]
    qtys = [o["quantity"] for o in seq]

    if len(set(pids)) != 1:
        return False
    if len(set(qtys)) != 1:
        return False

    return uids == sorted(uids) and len(uids) >= 3

# BOT MODE C:
# Random users BUT same product + same qty + consecutive
def detect_bot_mode_C(order):
    now = datetime.fromisoformat(order["event_ts"])

    recent = [
        o for o in BUFFER
        if now - datetime.fromisoformat(o["event_ts"]) < WINDOW
    ]

    if len(recent) < 2:
        return False

    seq = recent + [order]

    pids = [o["product_id"] for o in seq]
    qtys = [o["quantity"] for o in seq]

    if len(set(pids)) != 1:
        return False
    if len(set(qtys)) != 1:
        return False

    return True

def apply_fraud_rules(order):

    # Rule A: Country not ID
    if order.get("country") != "ID":
        return "Fraud"

    created_dt = datetime.fromisoformat(order["created_date"])
    hour = created_dt.hour

    # Rule B: Quantity anomaly
    if 0 <= hour < 4 and order.get("quantity", 0) > 100:
        return "Fraud"

    # Rule C: Amount anomaly
    if 0 <= hour < 4 and order.get("amount_numeric", 0) > 100_000_000:
        return "Fraud"

    # Rule D: Bot patterns
    if detect_bot_mode_A(order) or detect_bot_mode_B(order) or detect_bot_mode_C(order):
        return "Fraud"

    return "Genuine"

print("Fraud engine started... Listening to orders topic.")
try:
    for msg in consumer:
        order = msg.value

        BUFFER.append(order)

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