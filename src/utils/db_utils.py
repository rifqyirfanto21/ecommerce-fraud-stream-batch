from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
import pandas as pd
from typing import List, Dict, Any, Optional
from src.utils.config import DB_CONFIG

_engine: Optional[Engine] = None

def get_engine() -> Engine:
    """
    Database engine connection to PostgreSQL.
    """
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
    return _engine

def insert_users(users: List[Dict[str, Any]]):
    """
    Insert generated users data into raw_users table in PostgreSQL.
    """
    engine = get_engine()
    query = text(
        """
        INSERT INTO raw_users (name, email, phone_number, created_date)
        VALUES (:name, :email, :phone_number, :created_date)
        ON CONFLICT (email) DO NOTHING
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(query, users)
    except OperationalError:
        global _engine
        _engine = None
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(query, users)

def insert_products(products: List[Dict[str, Any]]):
    """
    Insert generated products data into raw_products table in PostgreSQL.
    """
    engine = get_engine()
    query = text(
        """
        INSERT INTO raw_products (product_name, brand, category, sub_category,
                                  currency, price, cost, created_date)
        VALUES (:product_name, :brand, :category, :sub_category,
                :currency, :price, :cost, :created_date)
        ON CONFLICT (product_name) DO NOTHING
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(query, products)
    except OperationalError:
        global _engine
        _engine = None
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(query, products)

def fetch_users_df(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Extract users table from raw_users in PostgreSQL as DataFrame.
    """
    engine = get_engine()
    sql = (
        "SELECT user_id, name, email, phone_number, created_date "
        "FROM raw_users ORDER BY user_id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def fetch_products_df(limit: Optional[int] = None) -> pd.DataFrame:
    """
    Extract products table from raw_products in PostgreSQL as DataFrame.
    """
    engine = get_engine()
    sql = """
        SELECT product_id, product_name, brand, category, sub_category,
               currency, price, cost, created_date
        FROM raw_products
        ORDER BY product_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def fetch_last_order_counter() -> int:
    """
    Extract numeric part of order_id from raw_orders.
    """
    engine = get_engine()
    sql = text(
        """
        SELECT MAX( (regexp_replace(order_id, '^O', ''))::BIGINT ) AS max_counter
        FROM raw_orders
        """
    )
    with engine.connect() as conn:
        res = conn.execute(sql).scalar()
    if res is None:
        return 0
    return int(res)

def upsert_processed_orders(orders: List[Dict[str, Any]]):
    """
    Batch upsert into raw_orders.
    """
    if not orders:
        return

    engine = get_engine()

    query = text("""
        INSERT INTO raw_orders (
            order_id, user_id, product_id, quantity,
            amount, amount_numeric, country, status,
            created_date, event_ts, source
        )
        VALUES (
            :order_id, :user_id, :product_id, :quantity,
            :amount, :amount_numeric, :country, :status,
            :created_date, :event_ts, :source
        )
        ON CONFLICT (order_id) DO UPDATE SET
            user_id        = EXCLUDED.user_id,
            product_id     = EXCLUDED.product_id,
            quantity       = EXCLUDED.quantity,
            amount         = EXCLUDED.amount,
            amount_numeric = EXCLUDED.amount_numeric,
            country        = EXCLUDED.country,
            status         = EXCLUDED.status,
            created_date   = EXCLUDED.created_date,
            event_ts       = EXCLUDED.event_ts,
            source         = EXCLUDED.source,
            ingestion_ts   = NOW()         -- refresh ingestion time
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query, orders)
    except OperationalError:
        global _engine
        _engine = None
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(query, orders)