# airflow/dags/batch_raw_loads.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from src.generator.batch_generator import generate_users, generate_products
from src.utils.db_utils import insert_users, insert_products

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

# Users DAG
def generate_users_task():
    return generate_users(50)

def load_users_task(ti):
    users = ti.xcom_pull(task_ids="generate_users")
    insert_users(users)

with DAG(
    dag_id="batch_raw_users_load",
    description="DAG to load raw users table in PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["raw", "postgres", "users"],
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    u1 = PythonOperator(
        task_id="generate_users",
        python_callable=generate_users_task,
    )

    u2 = PythonOperator(
        task_id="load_users_to_db",
        python_callable=load_users_task,
    )

    start >> u1 >> u2 >> end

# Products DAG
def generate_products_task():
    return generate_products(50)

def load_products_task(ti):
    products = ti.xcom_pull(task_ids="generate_products")
    insert_products(products)

with DAG(
    dag_id="batch_raw_products_load",
    description="DAG to load raw products table in PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["raw", "postgres", "products"],
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    p1 = PythonOperator(
        task_id="generate_products",
        python_callable=generate_products_task,
    )

    p2 = PythonOperator(
        task_id="load_products_to_db",
        python_callable=load_products_task,
    )

    start >> p1 >> p2 >> end