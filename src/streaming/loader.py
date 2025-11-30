import json
import time
from typing import List, Dict, Any
from kafka import KafkaConsumer
from datetime import datetime, timezone
from src.utils.db_utils import upsert_processed_orders
from src.utils.config import DB_CONFIG

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "processed_orders"

# batching params
BATCH_SIZE = int((__import__("os").environ.get("LOADER_BATCH_SIZE") or 100))
FLUSH_INTERVAL = float((__import__("os").environ.get("LOADER_FLUSH_INTERVAL") or 5.0))  # seconds

def parse_order(msg_value: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure the order dict has the shape expected by upsert_processed_orders.
    - order_id: "O123"
    - user_id: now kept as full string ID (e.g., "U123")
    - product_id: now kept as full string ID (e.g., "P123")
    - created_date, event_ts kept as ISO strings
    """
    o = dict(msg_value)  # copy

    # PERUBAHAN KRITIS: Hentikan konversi ke INT. Pertahankan sebagai string.
    # user_id dan product_id harus tetap berupa string karena kolom DB sekarang VARCHAR.
    
    uid = o.get("user_id")
    pid = o.get("product_id")
    
    # Konversi UID/PID ke string, jika belum (misalnya jika datang dari data lama)
    try:
        o["user_id"] = str(uid)
    except Exception:
        # Jika gagal konversi ke string, biarkan nilai aslinya
        pass

    try:
        o["product_id"] = str(pid)
    except Exception:
        # Jika gagal konversi ke string, biarkan nilai aslinya
        pass
    
    # Kolom numerik tetap harus dijamin int
    try:
        o["quantity"] = int(o.get("quantity", 0))
        o["amount_numeric"] = int(o.get("amount_numeric", 0))
    except Exception:
        pass

    # ensure created_date/event_ts are strings (ISO)
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
        auto_offset_reset="latest",  # don't reprocess older messages on restart
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

                # flush by size
                if len(batch) >= BATCH_SIZE:
                    upsert_processed_orders(batch)
                    print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (by size).")
                    batch.clear()
                    last_flush = time.time()

                # check interval flush
                if time.time() - last_flush >= FLUSH_INTERVAL:
                    if batch:
                        upsert_processed_orders(batch)
                        print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (by interval).")
                        batch.clear()
                    last_flush = time.time()

            # no messages on topic right now -> periodic flush if any
            if batch and (time.time() - last_flush >= FLUSH_INTERVAL):
                upsert_processed_orders(batch)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Flushed {len(batch)} orders (idle interval).")
                batch.clear()
                last_flush = time.time()

            # small sleep to avoid busy-loop
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