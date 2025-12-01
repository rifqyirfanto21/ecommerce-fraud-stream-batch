CREATE TABLE IF NOT EXISTS ingestion_monitoring (
    id SERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_status TEXT NOT NULL,
    source_table TEXT,
    row_ingested BIGINT,
    destination_table TEXT,
    row_loaded BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    execution_date TIMESTAMP NOT NULL,
    is_consistent BOOLEAN
);