import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.dummy import DummyOperator

GCS_BUCKET = "rifqy_ecommerce_finpro"
BQ_PROJECT = "jcdeah-006"
BQ_DS = "rifqy_ecommerce_finpro"

TABLES =  [
    "raw_users",
    "raw_products",
    "raw_orders"
]

SCHEMA_MAP = {
    "raw_users": [
        {"name": "user_id",      "type": "INT64",    "mode": "REQUIRED"},
        {"name": "name",         "type": "STRING",   "mode": "REQUIRED"},
        {"name": "email",        "type": "STRING",   "mode": "REQUIRED"},
        {"name": "phone_number", "type": "STRING",   "mode": "NULLABLE"},
        {"name": "created_date", "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "ingestion_ts", "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "source",       "type": "STRING",   "mode": "REQUIRED"},
    ],

    "raw_products": [
        {"name": "product_id",   "type": "INT64",    "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING",   "mode": "REQUIRED"},
        {"name": "brand",        "type": "STRING",   "mode": "NULLABLE"},
        {"name": "category",     "type": "STRING",   "mode": "NULLABLE"},
        {"name": "sub_category", "type": "STRING",   "mode": "NULLABLE"},
        {"name": "currency",     "type": "STRING",   "mode": "NULLABLE"},
        {"name": "price",        "type": "INT64",    "mode": "REQUIRED"},
        {"name": "cost",         "type": "INT64",    "mode": "REQUIRED"},
        {"name": "created_date", "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "ingestion_ts", "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "source",       "type": "STRING",   "mode": "REQUIRED"},
    ],

    "raw_orders": [
        {"name": "order_id",       "type": "STRING",   "mode": "REQUIRED"},
        {"name": "user_id",        "type": "STRING",   "mode": "REQUIRED"},
        {"name": "product_id",     "type": "STRING",   "mode": "REQUIRED"},
        {"name": "quantity",       "type": "INT64",    "mode": "REQUIRED"},
        {"name": "amount",         "type": "STRING",   "mode": "REQUIRED"},
        {"name": "amount_numeric", "type": "INT64",    "mode": "REQUIRED"},
        {"name": "country",        "type": "STRING",   "mode": "REQUIRED"},
        {"name": "status",         "type": "STRING",   "mode": "REQUIRED"},
        {"name": "created_date",   "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "event_ts",       "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "source",         "type": "STRING",   "mode": "REQUIRED"},
        {"name": "ingestion_ts",   "type": "TIMESTAMP","mode": "REQUIRED"},
    ]
}

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="postgres_to_bigquery_raw_tables",
    description="DAG to Incremental ELT raw tables pipeline insertion from PostgreSQL to BigQuery",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["raw", "bigquery"],
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    for table in TABLES:
        pg_to_gcs = PostgresToGCSOperator(
            task_id=f"pg_to_gcs_{table}",
            postgres_conn_id="postgres_default",
            # sql=f"""
            #     SELECT * FROM {table}
            #     WHERE DATE(created_date) = DATE('{{{{ ds }}}}'::TIMESTAMP - INTERVAL '1 day');
            #     """,
            sql=f"SELECT * FROM {table}",
            bucket=GCS_BUCKET,
            filename=f"raw/{table}_{{{{ ds }}}}.json",
            export_format="JSON",
            gcp_conn_id="google_cloud_default",
            )
        
        wait_for_gcs = GCSObjectExistenceSensor(
            task_id=f"wait_for_{table}",
            bucket=GCS_BUCKET,
            object=f"raw/{table}_{{{{ ds }}}}.json",
            google_cloud_conn_id="google_cloud_default",
            poke_interval=30,
            timeout=600,
            mode="poke",
            )
        
        gcs_to_bq = GCSToBigQueryOperator(
            task_id=f"gcs_to_bq_{table}",
            bucket=GCS_BUCKET,
            source_objects=[f"raw/{table}_{{{{ ds }}}}.json"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DS}.{table}",
            source_format="NEWLINE_DELIMITED_JSON",
            schema_fields=SCHEMA_MAP[table],
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning={"type": "DAY", "field": "created_date"},
            gcp_conn_id="google_cloud_default",
            )
        
        gcs_cleanup = GCSDeleteObjectsOperator(
            task_id=f"gcs_{table}_cleanup",
            bucket_name=GCS_BUCKET,
            objects=[f"raw/{table}_{{{{ ds }}}}.json"],
            trigger_rule="all_done",
            )
        
        start >> pg_to_gcs >> wait_for_gcs >> gcs_to_bq >> gcs_cleanup >> end