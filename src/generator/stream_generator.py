import random
import time
from math import ceil
from datetime import datetime, timezone
from typing import Callable, Optional
from src.utils.db_utils import fetch_users_df, fetch_products_df, fetch_last_order_counter

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

def format_rupiah(n: int) -> str:
    """
    Format an integer as Rupiah currency string.
    """
    return f"Rp.{n:,}".replace(",", ".")

def pick_country() -> str:
    """
    Random pick a country based on the predefined weights.
    """
    return random.choices(COUNTRIES, weights=WEIGHTS, k=1)[0]

def choose_quantity_normal() -> int:
    """
    Choose a normal quantity based on predefined probabilities.
    """
    r = random.random()
    if r < 0.7:
        return 1
    if r < 0.9:
        return 2
    return random.randint(3, 5)

def force_qty_over_100() -> int:
    """
    Force quantity to be over 100 for fabricated fraud.
    """
    return random.randint(101, 300)

def choose_amount_anomaly_qty(price: int) -> int:
    """
    Choose quantity to create an amount anomaly based on price for fabricated fraud.
    """
    if price <= 0:
        return 101
    needed = ceil(100_000_000 / price)
    if needed <= 100:
        return needed
    return random.randint(101, 300)

def generate_bot_sequence(user_ids):
    """
    Random generate for fabricated bot sequence.
    """
    mode = random.choice(["single_user_burst", "sequential_users", "clustered"])
    if mode == "single_user_burst":
        u = random.choice(user_ids)
        return mode, [u] * random.randint(3, 5)
    if mode == "sequential_users":
        sample_size = min(len(user_ids), random.randint(3, 6))
        seq = sorted(random.sample(user_ids, sample_size))
        return mode, seq
    sample_size = min(len(user_ids), random.randint(3, 5))
    seq = random.sample(user_ids, sample_size)
    return mode, seq

def fabricate_midnight_timestamp_same_date(now: datetime) -> datetime:
    """
    Generate random time between midnight and 3 AM on the same date for fabricated fraud.
    """
    date_part = now.date()
    hour = random.randint(0, 3)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year=date_part.year, month=date_part.month, day=date_part.day,
                    hour=hour, minute=minute, second=second, tzinfo=timezone.utc)

def continuous_orders_stream(
    send_func: Optional[Callable[[dict], None]] = None,
    sleep_range: tuple = (0.3, 1.5),
    bot_burst_probability: float = 0.10,
    qty_anomaly_probability: float = 0.02,
    amount_anomaly_probability: float = 0.02,
    refresh_interval_seconds: int = 3600,
):
    """
    Orders event stream generator.
    """
    # send_func: callable(order_dict), default -> print
    send = send_func or (lambda o: print(o))

    # initial load from Postgres raw tables
    users_df = fetch_users_df()
    products_df = fetch_products_df()

    # wait until batch data exists
    while users_df.empty or products_df.empty:
        print("Waiting for raw_users/raw_products in Postgres...")
        time.sleep(5)
        users_df = fetch_users_df()
        products_df = fetch_products_df()

    user_ids = users_df["user_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    price_map = dict(zip(products_df["product_id"], products_df["price"]))

    last_refresh = time.time()
    order_counter = fetch_last_order_counter() or 0
    order_counter += 1

    try:
        while True:
            now = datetime.now(timezone.utc)

            # refresh users/products periodically
            if time.time() - last_refresh > refresh_interval_seconds:
                users_df = fetch_users_df()
                products_df = fetch_products_df()
                if not users_df.empty and not products_df.empty:
                    user_ids = users_df["user_id"].tolist()
                    product_ids = products_df["product_id"].tolist()
                    price_map = dict(zip(products_df["product_id"], products_df["price"]))
                    last_refresh = time.time()
                    print("Refreshed users/products cache from Postgres")

            # BOT burst fabrication
            if random.random() < bot_burst_probability and len(user_ids) >= 3:
                mode, bot_seq = generate_bot_sequence(user_ids)
                bot_pid = random.choice(product_ids)
                bot_price = int(price_map[bot_pid])
                fixed_qty = choose_quantity_normal()

                for u in bot_seq:
                    if mode == "single_user_burst":
                        qty = choose_quantity_normal()
                    else:
                        qty = fixed_qty

                    amount_numeric = bot_price * qty
                    created_ts = datetime.now(timezone.utc)

                    order = {
                        "order_id": f"O{order_counter}",
                        "user_id": f"U{u}",
                        "product_id": f"P{bot_pid}",
                        "quantity": int(qty),
                        "amount": format_rupiah(amount_numeric),
                        "amount_numeric": int(amount_numeric),
                        "country": pick_country(),
                        "status": "pending",
                        "created_date": created_ts.replace(microsecond=0).isoformat(),
                        "event_ts": created_ts.isoformat(),
                        "source": "streaming"
                    }
                    order_counter += 1
                    send(order)
                    time.sleep(random.uniform(0.1, 0.3))
                continue

            # normal flow
            user = random.choice(user_ids)
            product = random.choice(product_ids)
            price = int(price_map[product])

            quantity = choose_quantity_normal()

            if random.random() < qty_anomaly_probability:
                quantity = force_qty_over_100()
                created_ts = fabricate_midnight_timestamp_same_date(now)
            elif random.random() < amount_anomaly_probability:
                quantity = choose_amount_anomaly_qty(price)
                created_ts = fabricate_midnight_timestamp_same_date(now)
            else:
                created_ts = datetime.now(timezone.utc)

            amount_numeric = price * quantity

            order = {
                "order_id": f"O{order_counter}",
                "user_id": f"U{user}",
                "product_id": f"P{product}",
                "quantity": int(quantity),
                "amount": format_rupiah(amount_numeric),
                "amount_numeric": int(amount_numeric),
                "country": pick_country(),
                "status": "pending",
                "created_date": created_ts.replace(microsecond=0).isoformat(),
                "event_ts": created_ts.isoformat(),
                "source": "streaming"
            }

            order_counter += 1
            send(order)

            time.sleep(random.uniform(*sleep_range))

    except KeyboardInterrupt:
        print("stream stopped by user")