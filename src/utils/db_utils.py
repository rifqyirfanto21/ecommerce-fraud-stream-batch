# src/utils/db_utils.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
import pandas as pd
from typing import List, Dict, Any, Optional

# load config (tries project-local src.utils.config then top-level src.config)
try:
    from src.utils.config import DB_CONFIG  # type: ignore
except Exception:
    from src.config import DB_CONFIG  # type: ignore

_engine: Optional[Engine] = None

def get_engine() -> Engine:
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
            pool_recycle=1800,  # recycle every 30 mins
        )
    return _engine

def insert_users(users: List[Dict[str, Any]]):
    # insert batch into raw_users; dedupe by name
    engine = get_engine()
    query = text(
        """
        INSERT INTO raw_users (name, email, phone_number, created_date)
        VALUES (:name, :email, :phone_number, :created_date)
        ON CONFLICT (name) DO NOTHING
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
    # insert batch into raw_products; dedupe by product_name
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
    # return user_id (int) and other fields
    engine = get_engine()
    sql = "SELECT user_id, name, email, phone_number, created_date FROM raw_users ORDER BY user_id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return pd.read_sql(sql, con=engine)

def fetch_products_df(limit: Optional[int] = None) -> pd.DataFrame:
    # return product_id (int) and price etc
    engine = get_engine()
    sql = """SELECT product_id, product_name, brand, category, sub_category,
                    currency, price, cost, created_date
             FROM raw_products
             ORDER BY product_id"""
    if limit:
        sql += f" LIMIT {int(limit)}"
    return pd.read_sql(sql, con=engine)

def fetch_last_order_counter() -> int:
    # extract numeric part of order_id like 'O1234' from raw_orders
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