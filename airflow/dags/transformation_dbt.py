from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from utils.notif_callbacks import dag_success_notif_message, failed_notif_message

default_args = {
    "owner": "rifqy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="dbt_run",
    description="DBT DAG to transform tables in BigQuery",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "dbt", "transformation", "dim", "fct", "marts"],
    on_failure_callback=failed_notif_message
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir /opt/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir /opt/dbt",
    )

    dbt_dag_success = PythonOperator(
        task_id="dbt_dag_success",
        python_callable=dag_success_notif_message,
    )

    start >> dbt_run >> dbt_test >> dbt_dag_success >> end