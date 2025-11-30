import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator

from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "jcdeah-006"
BQ_DS = "rifqy_ecommerce_finpro"

SERVICE_ACCOUNT_PATH = "/opt/gcp/service-account.json"

TABLES = ["raw_users", "raw_products", "raw_orders"]

SCHEMA_MAP = {
    "raw_users": [
        {"name": "user_id", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "phone_number", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "created_date", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingestion_ts", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source", "field_type": "STRING", "mode": "REQUIRED"},
    ],
    "raw_products": [
        {"name": "product_id", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "product_name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "brand", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "sub_category", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "currency", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "cost", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "created_date", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingestion_ts", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source", "field_type": "STRING", "mode": "REQUIRED"},
    ],
    "raw_orders": [
        {"name": "order_id", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "product_id", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "quantity", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "amount", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "amount_numeric", "field_type": "INT64", "mode": "REQUIRED"},
        {"name": "country", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "status", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "created_date", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_ts", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "ingestion_ts", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def export_postgres_to_local(table, **context):
    pg = PostgresHook(postgres_conn_id="postgres_default")
    df = pg.get_pandas_df(f"SELECT * FROM {table}")

    temp_path = f"/tmp/{table}.json"
    df.to_json(temp_path, orient="records", lines=True)
    return temp_path

def load_json_to_bigquery(table, **context):
    json_path = f"/tmp/{table}.json"

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH
    )

    client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

    table_ref = f"{BQ_PROJECT}.{BQ_DS}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        schema=[bigquery.SchemaField(**field) for field in SCHEMA_MAP[table]],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date",
        ),
    )

    with open(json_path, "rb") as f:
        job = client.load_table_from_file(
            f, destination=table_ref, job_config=job_config
        )
    job.result()

with DAG(
    dag_id="postgres_to_bq_explicit_credentials",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["raw", "bigquery"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    for table in TABLES:

        extract = PythonOperator(
            task_id=f"extract_{table}",
            python_callable=export_postgres_to_local,
            op_kwargs={"table": table},
        )

        load = PythonOperator(
            task_id=f"load_{table}_to_bq",
            python_callable=load_json_to_bigquery,
            op_kwargs={"table": table},
        )

        cleanup = PythonOperator(
            task_id=f"cleanup_{table}",
            python_callable=lambda table=table: os.remove(f"/tmp/{table}.json"),
        )

        start >> extract >> load >> cleanup >> end