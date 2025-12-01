from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from google.cloud import bigquery

def get_metadata_connection():
    """
    Create Airflow metadata DB Connection
    """
    conn = BaseHook.get_connection("postgres_metadata")
    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
    return engine

def get_bq_client():
    """
    Create BigQuery Client using ADC (Application Default Credentials)
    """
    client = bigquery.Client(project="jcdeah-006")
    return client