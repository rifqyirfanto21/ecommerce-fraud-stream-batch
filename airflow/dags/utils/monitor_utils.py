import logging
from datetime import datetime
from sqlalchemy import text
from airflow.utils.context import Context
from utils.db_utils_airflow import get_metadata_connection, get_bq_client
from src.utils.db_utils import get_engine as get_db_connection
from utils.notif_callbacks import consistent_notif_message, inconsistent_notif_message, failed_notif_message

def monitor_start(source_table: str, destination_table: str, is_incremental=True, **context: Context):
    """
    Starts monitoring ingestion by recording the number of rows to be ingested from source_table. Entries are stored in the ingestion_monitoring table in the metadata database.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    run_status = "RUNNING"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execution_date = context["ds"]

    try:
        if is_incremental:
            where_clause = f"WHERE DATE(created_date) = DATE('{execution_date}'::timestamp - interval '1 day')"
        else:
            where_clause = ""

        logging.info(f"[START] Monitoring ingestion for table: {source_table} to {destination_table}")
        warehouse_engine = get_db_connection()
        with warehouse_engine.connect() as wh_conn:
            result = wh_conn.execute(text(f"""
                SELECT COUNT(*) FROM {source_table}
                {where_clause}
            """))
            row_ingested = result.scalar()

        metadata_engine = get_metadata_connection()
        with metadata_engine.connect() as meta_conn:
            meta_conn.execute(text("""
                INSERT INTO ingestion_monitoring (
                    dag_id, run_id, run_status,
                    source_table, row_ingested,
                    destination_table, start_time, execution_date
                ) VALUES (
                    :dag_id, :run_id, :run_status,
                    :source_table, :row_ingested, :destination_table,
                    :start_time, :execution_date)
                """), {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "run_status": run_status,
                    "source_table": source_table,
                    "row_ingested": row_ingested,
                    "destination_table": destination_table,
                    "start_time": start_time,
                    "execution_date": execution_date
                })
        logging.info(f"[START] Monitoring completed — {row_ingested} rows detected in {source_table}")

    except Exception as e:
        logging.error(f"[ERROR] Monitoring {source_table} to {destination_table} failed: {e}")
        raise

def monitor_finish(source_table: str, destination_table: str, bq_project: str, bq_dataset: str, is_incremental=True, **context: Context):
    """
    Finish monitoring ingestion by recording the number of rows ingested from source_table to destination_table in BigQuery. Updates the ingestion_monitoring table in the metadata database.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    run_status = "SUCCESS"
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execution_date = context["ds"]

    try:
        if is_incremental:
            where_clause = f"WHERE DATE(created_date) = DATE('{execution_date}') - 1"
        else:
            where_clause = ""

        logging.info(f"[FINISH] Monitoring ingestion for table: {source_table} to {destination_table}")
        bq_client = get_bq_client()
        query = f"""
            SELECT COUNT(*) FROM `{bq_project}.{bq_dataset}.{destination_table}`
            {where_clause}
        """
        bq_result = bq_client.query(query).result()
        row_loaded = list(bq_result)[0][0]

        metadata_engine = get_metadata_connection()
        with metadata_engine.connect() as meta_conn:
            meta_conn.execute(text("""
                UPDATE ingestion_monitoring
                SET row_loaded = :row_loaded,
                    run_status = :run_status,
                    end_time = :end_time
                WHERE dag_id = :dag_id AND run_id = :run_id
                AND source_table = :source_table
            """), {
                "row_loaded": row_loaded,
                "run_status": run_status,
                "end_time": end_time,
                "dag_id": dag_id,
                "run_id": run_id,
                "source_table": source_table
            })
        logging.info(f"[FINISH] Monitoring completed — {row_loaded} rows loaded into {destination_table}")

    except Exception as e:
        logging.error(f"[ERROR] Monitoring {source_table} to {destination_table} failed: {e}")
        raise

def monitor_validate(source_table: str, destination_table: str, **context: Context):
    """
    Validate monitoring ingestion by comparing row_ingested vs row_loaded. Updates the ingestion_monitoring table in the metadata database.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    try:
        logging.info(f"[VALIDATE] Comparing row_ingested vs row_loaded for {source_table}")

        metadata_engine = get_metadata_connection()
        with metadata_engine.connect() as meta_conn:
            result = meta_conn.execute(text("""
                SELECT row_ingested, row_loaded
                FROM ingestion_monitoring
                WHERE dag_id = :dag_id
                AND run_id = :run_id
                AND source_table = :source_table
            """), {
                "dag_id": dag_id,
                "run_id": run_id,
                "source_table": source_table
            }).fetchone()

            if result:
                row_ingested, row_loaded = result
                is_consistent = row_ingested == row_loaded

                meta_conn.execute(text("""
                    UPDATE ingestion_monitoring
                    SET is_consistent = :is_consistent
                    WHERE dag_id = :dag_id
                    AND run_id = :run_id
                    AND source_table = :source_table
                """), {
                    "is_consistent": is_consistent,
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "source_table": source_table
                })

                if is_consistent:
                    consistent_notif_message(
                        context,
                        ingested=row_ingested,
                        loaded=row_loaded
                    )
                    logging.info(f"[VALIDATION] ✅ Row counts match: {row_ingested} = {row_loaded}")
                else:
                    inconsistent_notif_message(
                        context,
                        ingested=row_ingested,
                        loaded=row_loaded
                    )
                    logging.warning(f"[VALIDATION] ⚠️ Row mismatch: ingested {row_ingested}, loaded {row_loaded}")

            else:
                logging.warning(f"[VALIDATION] No monitoring entry found for {source_table}")

    except Exception as e:
        logging.error(f"[ERROR] Validation for {source_table} to {destination_table} failed: {e}")
        raise

def monitor_failed(context: Context):
    """
    Marks the ingestion as FAILED in the ingestion_monitoring table in the metadata database if any of the task fails.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execution_date = context["ds"]

    try:
        logging.warning(f"[FAILED] Marking ingestion as FAILED for DAG: {dag_id}")
        metadata_engine = get_metadata_connection()
        with metadata_engine.connect() as meta_conn:
            result = meta_conn.execute(text("""
                SELECT COUNT(*)
                FROM ingestion_monitoring
                WHERE dag_id = :dag_id AND run_id = :run_id                
            """), {
                "dag_id": dag_id,
                "run_id": run_id
            }).scalar()

            if result > 0:
                meta_conn.execute(text("""
                    UPDATE ingestion_monitoring
                    SET run_status = 'FAILED',
                        end_time = :end_time
                    WHERE dag_id = :dag_id AND run_id = :run_id
                """), {
                    "end_time": end_time,
                    "dag_id": dag_id,
                    "run_id": run_id
                })
                logging.info(f"[FAILED] Monitoring updated run_status to FAILED for DAG: {dag_id}")

            else:
                meta_conn.execute(text("""
                    INSERT INTO ingestion_monitoring (
                        dag_id, run_id, run_status,
                        start_time, end_time, execution_date, is_consistent
                    ) VALUES (
                        :dag_id, :run_id, 'FAILED', :start_time,
                        :end_time, :execution_date, FALSE)
                """), {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "start_time": end_time,
                    "end_time": end_time,
                    "execution_date": execution_date
                })
                logging.info(f"[FAILED] Monitoring created new FAILED entry for DAG: {dag_id}")

        failed_notif_message(context)

    except Exception as e:
        logging.error(f"[ERROR] Monitoring for {dag_id} — {run_id} failed: {e}")
        raise