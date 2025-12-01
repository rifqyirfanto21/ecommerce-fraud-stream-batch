import json
import time
from typing import List, Dict, Any
from kafka import KafkaConsumer
from datetime import datetime, timezone
from src.utils.db_utils import upsert_processed_orders
from src.utils.config import DB_CONFIG

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "processed_orders"

BATCH_SIZE = int((__import__("os").environ.get("LOADER_BATCH_SIZE") or 100))
FLUSH_INTERVAL = float((__import__("os").environ.get("LOADER_FLUSH_INTERVAL") or 5.0))

def parse_order(msg_value: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure the order dict has the shape expected by upsert_processed_orders.
    """
    o = dict(msg_value)

    uid = o.get("user_id")
    pid = o.get("product_id")
    
    try:
        o["user_id"] = str(uid)
    except Exception:
        pass

    try:
        o["product_id"] = str(pid)
    except Exception:
        pass

    try:
        o["quantity"] = int(o.get("quantity", 0))
        o["amount_numeric"] = int(o.get("amount_numeric", 0))
    except Exception:
        pass

    for k in ("created_date", "event_ts"):
        v = o.get(k)
        if hasattr(v, "isoformat"):
            o[k] = v.isoformat()
            
    return o

def main():
    print("Loader starting... connecting to Kafka:", KAFKA_BROKER)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    batch: List[Dict[str, Any]] = []
    last_flush = time.time()

    try:
        while True:
            for msg in consumer:
                order_raw = msg.value
                order = parse_order(order_raw)
                batch.append(order)

                if len(batch) >= BATCH_SIZE:
                    upsert_processed_orders(batch)
                    print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (by size).")
                    batch.clear()
                    last_flush = time.time()

                if time.time() - last_flush >= FLUSH_INTERVAL:
                    if batch:
                        upsert_processed_orders(batch)
                        print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (by interval).")
                        batch.clear()
                    last_flush = time.time()

            if batch and (time.time() - last_flush >= FLUSH_INTERVAL):
                upsert_processed_orders(batch)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (idle interval).")
                batch.clear()
                last_flush = time.time()

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Loader stopping by user, flushing remaining ...")
        if batch:
            upsert_processed_orders(batch)
            print(f"Flushed {len(batch)} remaining orders.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()