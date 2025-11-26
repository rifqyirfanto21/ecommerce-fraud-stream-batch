import random
import time
from math import ceil
from datetime import datetime, timezone
from typing import Callable, Optional

COUNTRY_WEIGHTS = {
    "ID": 0.95,
    "SG": 0.007,
    "MY": 0.007,
    "TH": 0.007,
    "VN": 0.007,
    "PH": 0.007,
    "KR": 0.007,
    "JP": 0.007
}
COUNTRIES = list(COUNTRY_WEIGHTS.keys())
WEIGHTS = list(COUNTRY_WEIGHTS.values())

order_counter = 1

def format_rupiah(n: int) -> str:
    return f"Rp.{n:,}".replace(",", ".")

def pick_country() -> str:
    return random.choices(COUNTRIES, weights=WEIGHTS, k=1)[0]

def generate_order_id() -> str:
    global order_counter
    oid = f"O{order_counter}"
    order_counter += 1
    return oid

def in_midnight_window_utc(dt: datetime) -> bool:
    h = dt.hour
    return 0 <= h < 4

def choose_quantity_normal() -> int:
    r = random.random()
    if r < 0.7:
        return 1
    if r < 0.9:
        return 2
    return random.randint(3, 5)

def force_qty_over_100() -> int:
    return random.randint(101, 300)

def choose_amount_anomaly_qty(price: int) -> int:
    if price <= 0:
        return 101
    needed = ceil(100_000_000 / price)
    if needed <= 100:
        return needed
    return random.randint(101, 300)

def generate_bot_sequence(user_ids):
    mode = random.choice(["single_user_burst", "sequential_users", "clustered"])

    if mode == "single_user_burst":
        u = random.choice(user_ids)
        return mode, [u] * random.randint(3, 5)

    if mode == "sequential_users":
        sample_size = min(len(user_ids), random.randint(3, 6))
        seq = sorted(random.sample(user_ids, sample_size))
        return mode, seq

    # Mode C: random users (clustered)
    sample_size = min(len(user_ids), random.randint(3, 5))
    seq = random.sample(user_ids, sample_size)
    return mode, seq


def fabricate_midnight_timestamp_same_date(now: datetime) -> datetime:
    date_part = now.date()
    hour = random.randint(0, 3)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year=date_part.year, month=date_part.month, day=date_part.day,
                    hour=hour, minute=minute, second=second, tzinfo=timezone.utc)


def continuous_orders_stream(
    users_df,
    products_df,
    send_func: Optional[Callable[[dict], None]] = None,
    sleep_range: tuple = (0.3, 1.5),
    bot_burst_probability: float = 0.10,
    qty_anomaly_probability: float = 0.02,
    amount_anomaly_probability: float = 0.02
):

    user_ids = users_df["user_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    price_map = dict(zip(products_df["product_id"], products_df["price"]))
    send = send_func or (lambda o: print(o))

    try:
        while True:
            now = datetime.now(timezone.utc)

            # BOT BURST FABRICATION
            if random.random() < bot_burst_probability and len(user_ids) >= 3:
                mode, bot_seq = generate_bot_sequence(user_ids)

                # choose product ONCE for whole burst
                bot_pid = random.choice(product_ids)
                bot_price = int(price_map[bot_pid])

                # qty only fixed for mode B & C
                fixed_qty = choose_quantity_normal()

                for u in bot_seq:

                    if mode == "single_user_burst":
                        qty = choose_quantity_normal()
                    else:
                        qty = fixed_qty

                    amount_numeric = bot_price * qty

                    created_ts = datetime.now(timezone.utc)

                    order = {
                        "order_id": generate_order_id(),
                        "user_id": f"U{u}",
                        "product_id": f"P{bot_pid}",
                        "quantity": qty,
                        "amount": format_rupiah(amount_numeric),
                        "amount_numeric": amount_numeric,
                        "country": pick_country(),
                        "status": "pending",
                        "created_date": created_ts.replace(microsecond=0).isoformat(),
                        "event_ts": created_ts.isoformat(),
                        "source": "streaming"
                    }

                    send(order)
                    time.sleep(random.uniform(0.1, 0.3))

                continue

            # NORMAL ORDER FLOW
            user = random.choice(user_ids)
            product = random.choice(product_ids)
            price = int(price_map[product])

            quantity = choose_quantity_normal()

            if random.random() < qty_anomaly_probability:
                quantity = force_qty_over_100()
                fabricated_ts = fabricate_midnight_timestamp_same_date(now)
                created_ts = fabricated_ts

            elif random.random() < amount_anomaly_probability:
                quantity = choose_amount_anomaly_qty(price)
                fabricated_ts = fabricate_midnight_timestamp_same_date(now)
                created_ts = fabricated_ts

            else:
                created_ts = datetime.now(timezone.utc)

            amount_numeric = price * quantity

            order = {
                "order_id": generate_order_id(),
                "user_id": f"U{user}",
                "product_id": f"P{product}",
                "quantity": quantity,
                "amount": format_rupiah(amount_numeric),
                "amount_numeric": amount_numeric,
                "country": pick_country(),
                "status": "pending",
                "created_date": created_ts.replace(microsecond=0).isoformat(),
                "event_ts": created_ts.isoformat(),
                "source": "streaming"
            }

            send(order)
            time.sleep(random.uniform(*sleep_range))

    except KeyboardInterrupt:
        print("stream stopped by user")