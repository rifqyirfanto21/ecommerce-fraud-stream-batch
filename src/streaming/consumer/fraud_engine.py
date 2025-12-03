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

BUFFER = deque(maxlen=200)
WINDOW = timedelta(seconds=10)


def detect_bot_mode_A(order):
    """
    Mode A: same user_id + same product_id + 3 consecutive orders within WINDOW.
    """
    user = order["user_id"]
    pid = order["product_id"]
    now = datetime.fromisoformat(order["event_ts"])

    recent = [
        o for o in BUFFER
        if o.get("user_id") == user and o.get("product_id") == pid
        and now - datetime.fromisoformat(o["event_ts"]) < WINDOW
    ]
    return len(recent) >= 2


def detect_bot_mode_B(order):
    """
    Mode B: sequential (ascending) numeric user_id sequence, same product, same qty, consecutive
    """
    now = datetime.fromisoformat(order["event_ts"])

    recent = [o for o in BUFFER
              if now - datetime.fromisoformat(o["event_ts"]) < WINDOW]

    if len(recent) < 2:
        return False

    seq = recent + [order]

    try:
        uids = [int(o["user_id"]) for o in seq]
    except Exception:
        return False

    pids = [o["product_id"] for o in seq]
    qtys = [o["quantity"] for o in seq]

    if len(set(pids)) != 1:
        return False
    if len(set(qtys)) != 1:
        return False

    return uids == sorted(uids) and len(uids) >= 3


def detect_bot_mode_C(order):
    """
    Mode C: different random user_ids but same product and same qty, consecutive
    """
    now = datetime.fromisoformat(order["event_ts"])

    recent = [o for o in BUFFER
              if now - datetime.fromisoformat(o["event_ts"]) < WINDOW]

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
    """
    Return 'Fraud' or 'Genuine'
    Rules:
      - Country != 'ID' => Fraud
      - Quantity > 100 and created_date hour in [0,4) UTC => Fraud
      - Amount_numeric > 100_000_000 and created_date hour in [0,4) UTC => Fraud
      - Bot patterns A/B/C => Fraud
    """
    # Rule A
    if order.get("country") != "ID":
        return "Fraud"

    created_dt = datetime.fromisoformat(order["created_date"])
    hour = created_dt.hour

    # Rule B
    if 0 <= hour < 4 and order.get("quantity", 0) > 100:
        return "Fraud"

    # Rule C
    if 0 <= hour < 4 and order.get("amount_numeric", 0) > 100_000_000:
        return "Fraud"

    # Rule D: bot detection
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