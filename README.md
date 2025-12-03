# End-to-End Modern Data Platform: Batch, Streaming, Fraud Detection & ELT

**Author:** Muhammad Rifqy Irfanto  
**Program:** Purwadhika Data Engineering Bootcamp  
**Project:** Final Project (finpro)

---

## 📌 Project Overview

This repository implements a **modern, end-to-end data platform** that simulates an e-commerce environment with:

- **Batch processing** for Users and Products  
- **Real-time streaming** of Orders via Kafka  
- A **Fraud Detection Engine** with multiple business rules  
- A **Loader microservice** that writes processed orders into PostgreSQL  
- **ELT ingestion** from PostgreSQL → Google Cloud Storage → BigQuery  
- **dbt transformations** across staging, intermediate, and marts layers  
- **Data quality checks & notifications** via Discord webhooks  
- Full **orchestration with Apache Airflow**

The design closely mirrors production architectures used in **e-commerce**, **fintech**, and other **data-intensive applications**.

---

## 📑 Table of Contents

- [Project Overview](#-project-overview)  
- [Data Sources](#-data-sources)  
- [Features](#-features)  
- [Architecture Overview](#-architecture-overview)  
- [Pipeline Processes](#-pipeline-processes)  
  - [Batch ETL](#batch-etl)  
  - [Streaming Pipeline](#streaming-pipeline)  
  - [Fraud Detection](#fraud-detection)  
  - [PostgreSQL → GCS → BigQuery ELT](#postgresql--gcs--bigquery-elt)  
  - [dbt Transformations](#dbt-transformations)  
- [Order Schema](#-order-schema)  
- [Project Structure](#-project-structure)  
- [Technologies Used](#-technologies-used)  
- [Airflow DAGs](#-airflow-dags)  
- [Monitoring & Notifications](#-monitoring--notifications)  
- [Getting Started](#-getting-started)  
- [Docker Usage](#-docker-usage)  
- [Configuration](#-configuration)  
- [Author & Acknowledgments](#-author--acknowledgments)

---

## 📂 Data Sources

All data in this project is **synthetic** and generated programmatically.

### Batch Data

Generated in `src/generator/batch_generator.py`:

- Users  
- Products  

These entities are loaded into PostgreSQL as:

- `raw_users`  
- `raw_products`  

They behave like **dimension-style tables** and act as the reference backbone for the streaming pipeline.

### Streaming Data

Generated in `src/generator/stream_generator.py`:

- Order events referencing existing `user_id` and `product_id`  
- Published to Kafka topic `orders`  

No external CSVs are required; the full environment can be bootstrapped from code.

---

## ⭐ Features

- Synthetic data generation for Users, Products, and Orders  
- Batch loading into PostgreSQL with idempotent semantics  
- Kafka-based streaming pipeline with producers and consumers  
- Multi-rule Fraud Detection Engine with event enrichment  
- Loader microservice that persists processed orders into PostgreSQL  
- ELT from PostgreSQL → GCS → BigQuery  
- dbt-based modeling (staging, intermediate, marts) in BigQuery  
- Row-count based data quality checks  
- Discord webhook notifications for pipeline status and consistency  
- Full orchestration using Apache Airflow DAGs  

---

## 🏗 Architecture Overview

The platform is organized into three primary flows:

### 1. Batch Data Pipeline

- Synthetic **Users** and **Products** are generated via Python.  
- Data is loaded into PostgreSQL (`raw_users`, `raw_products`) using idempotent logic.  
- These tables function as slowly changing dimensions and serve as reference data for streaming orders.

### 2. Streaming Data Pipeline

- Orders are generated continuously and published to Kafka topic `orders`.  
- The **Fraud Detection Engine** consumes from `orders`, enriches events, evaluates rules, and outputs to:
  - `processed_orders` (all events with classification and enrichment)  
  - `fraud_alert` (fraud-only events)  
- The **Loader microservice** consumes `processed_orders` and inserts the results into PostgreSQL `raw_orders`.

### 3. ELT and Warehouse Modeling

- Raw tables in PostgreSQL (`raw_users`, `raw_products`, `raw_orders`) are exported to GCS.  
- GCS files are loaded into BigQuery raw tables.  
- dbt transforms raw data into staging, intermediate (dim/fact), and marts layers.  
- Airflow orchestrates extraction, loading, transformations, and monitoring end to end.

---

## 🔄 Pipeline Processes

### Batch ETL

**Objective:**  
Simulate dimension-style entities and provide stable reference data for Orders.

**Key steps:**

- Generate synthetic Users & Products (Python, `batch_generator.py`).  
- Load into PostgreSQL tables:
  - `raw_users`  
  - `raw_products`  
- Use conflict handling on natural keys to ensure **idempotency** (re-runs do not create duplicates).  
- Orchestrated by Airflow DAG: `batch_raw_loads.py`.

---

### Streaming Pipeline

**Objective:**  
Model high-velocity order events and support real-time fraud detection.

#### 1. Producer (`orders_producer.py`)

Location: `src/streaming/producers/orders_producer.py`

Responsibilities:

- Read current Users and Products from PostgreSQL (`raw_users`, `raw_products`).  
- Generate synthetic Orders referencing existing `user_id` and `product_id`.  
- Compute basic fields like quantity and amount.  
- Publish events to Kafka topic `orders`.

#### 2. Fraud Engine (`fraud_engine.py`)

Location: `src/streaming/consumers/fraud_engine.py`

Responsibilities:

- Consume events from Kafka topic `orders`.  
- Enrich events with additional metadata required for fraud analysis.  
- Apply fraud rules (see [Fraud Detection](#fraud-detection)).  
- Update `status` from its initial value to either `Genuine` or `Fraud`.  
- Publish results to:
  - `processed_orders` (all screened events)  
  - `fraud_alert` (fraud-only events for monitoring and alerting)

#### 3. Loader (`loader.py`)

Location: `src/streaming/loader.py`

Responsibilities:

- Consume events from Kafka topic `processed_orders`.  
- Insert into PostgreSQL table `raw_orders`.  
- Maintain idempotency / ordering (e.g., by tracking last ingested `order_id`).  
- Provide a near real-time transactional table in PostgreSQL that mirrors processed Kafka events.

---

### Fraud Detection

The Fraud Detection Engine evaluates each order event based on a series of business rules designed to mimic real fraud monitoring patterns. Examples include:

1. **Country-based rule**  
   - If `country` is not `"ID"`, classify as **Fraud**.

2. **High-quantity nighttime rule**  
   - If `quantity` is above a 100 during late-night hours (00:00–04:00), classify as **Fraud**.

3. **High-amount nighttime rule**  
   - If `amount` exceeds 100,000,000 IDR and occurs at night (00:00–04:00), classify as **Fraud**.

4. **Repeated user and product rule**  
   - Three or more consecutive events with the same `user_id` and `product_id` may be classified as **bot-like** and flagged as **Fraud**.

5. **Sequential user pattern rule**  
   - Events with sequential `user_id` values, same product, and same quantity in rapid succession indicate suspicious scripted activity, classify as **Fraud**.

6. **Multiple users same product pattern rule**  
   - Multiple different users purchasing the same product with identical quantities in a narrow time frame can indicate coordinated fraud, classify as **Fraud**.

Each order is enriched, evaluated, and then assigned a final `status`:

- `Pending` — initial status before screening (on the `orders` topic).  
- `Genuine` — classified as legitimate.  
- `Fraud` — classified as suspicious / fraudulent.

Processed events are stored in `processed_orders` and relevant fraud-only events in `fraud_alert`.

---

### PostgreSQL → GCS → BigQuery ELT

**Objective:**  
Move consolidated batch and streaming data into BigQuery for analytics using a scalable, cloud-native ELT pattern.

**Key steps:**

1. Extract from PostgreSQL tables:
   - `raw_users`  
   - `raw_products`  
   - `raw_orders`  

2. Write data to **Google Cloud Storage** in a suitable file format.  

3. Load from GCS into **BigQuery** raw tables inside a dedicated dataset.

4. Trigger dbt transformations to build higher-level models.

All of this is orchestrated through the **`bigquery_ingest.py`** Airflow DAG.

---

### dbt Transformations

The dbt project lives in `finpro_dbt/` and is structured into three logical layers:

#### 1. Staging Layer (stg\_)

- Models: `stg_users`, `stg_products`, `stg_orders`  
- Responsibilities:
  - Standardize column names and types  
  - Normalize timestamps and formats  
  - Perform light data cleaning  
  - Provide a consistent, well-structured base for further transformations  

#### 2. Intermediate Layer (dim\_ and fct\_)

- Models: `dim_users`, `dim_products`, `fct_orders`  
- Responsibilities:
  - Build dimension and fact structures for analysis  
  - Normalize IDs and keys  
  - Enforce schemas and data types explicitly  
  - Apply partitioning / clustering strategies where suitable (e.g., on `created_date`)

#### 3. Marts Layer (mart\_)

- Examples of mart models:
  - `mart_fraud_attempt_trends`  
  - `mart_fraud_daily_saved_amount`  
  - `mart_high_risk_products`  
  - `mart_products_revenue_profit`  
  - `mart_top10_active_users`  
  - `mart_top10_selling_products`  
  - `mart_top10_users_fraud`  

These tables are **analytics-ready** and designed to support reporting, dashboards, fraud monitoring, and business insights.

---

## 🧾 Order Schema

The **Orders** table follows the required base schema from the project guidelines but is **open to enrichment** during the streaming and fraud detection processes. Below is the complete and updated version.

---

## 🧍‍♂️ Users Schema (Referenced by Orders)

### **Required Fields**
- **user_id** — primary key  
- **name**  
- **email**  
- **created_date**

### **Enriched Fields**
- **phone_number** — additional contact detail  
- **source** — data origin indicator (default: `"batch"`)

### **Final Users Schema (Combined)**
- user_id  
- name  
- email  
- phone_number  
- created_date  
- source  

---

## 📦 Products Schema (Referenced by Orders)

### **Required Fields**
- **product_id** — primary key  
- **product_name**  
- **category**  
- **price**  
- **created_date**

### **Enriched Fields**
- **sub_category** — refined product classification  
- **currency** — e.g., `"IDR"`  
- **cost** — cost of goods sold (useful for margin analytics)  
- **source** — ingestion origin (batch pipeline)

### **Final Products Schema (Combined)**
- product_id  
- product_name  
- category  
- sub_category  
- currency  
- price  
- cost  
- created_date  
- source  

---

## 🧾 Orders Schema (Updated & Combined)

### **Required Fields (Project Guideline Minimum)**  
These are the mandatory fields present in every order event across all layers:

- `order_id` – Primary key of the order.  
- `user_id` – Foreign key referencing Users.  
- `product_id` – Foreign key referencing Products.  
- `quantity` – Number of units ordered.  
- `amount` – Raw amount value (string).  
- `country` – Country code (e.g., `ID`).  
- `created_date` – Timestamp / date representing when the order was created.  
- `status` – Lifecycle state:
  - `Pending` when the event is first produced to `orders`.  
  - `Genuine` or `Fraud` after evaluation by the Fraud Engine.

### **Enriched Fields (Added by Streaming Pipeline & Fraud Engine)**

To make the events more realistic and usable for fraud detection and analytics, the pipeline enriches orders with additional fields such as:

- `amount_numeric` – Numeric representation of `amount` for efficient processing.  
- `event_ts` – Event-time timestamp.  
- `source` – Indicator of the upstream source or generator.  

### **Final Orders Schema (Combined)**  
The final schema after enrichment includes:

- order_id  
- user_id  
- product_id  
- quantity  
- amount  
- amount_numeric  
- country  
- created_date  
- event_ts  
- status  
- source

These fields are generated during streaming and transformation and are **not part of the strict minimum schema**, but enhance realism and analytical power.

---

## 📁 Project Structure

    finpro/
    │
    ├── docker/
    │   ├── airflow/
    │   │   ├── Dockerfile
    │   │   └── requirements-airflow.txt
    │   ├── loader/
    │   │   ├── Dockerfile
    │   │   └── requirements-loader.txt
    │   └── sql/
    │       ├── init.sql
    │       └── init_metadata.sql
    │
    ├── airflow/
    │   ├── dags/
    │   │   ├── batch_raw_loads.py
    │   │   ├── bigquery_ingest.py
    │   │   ├── transformation_dbt.py
    │   │   └── utils/
    │   │       ├── db_utils_airflow.py
    │   │       ├── monitor_utils.py
    │   │       └── notif_callbacks.py
    │   ├── logs/
    │   ├── plugins/
    │   └── config/
    │
    ├── src/
    │   ├── streaming/
    │   │   ├── producers/
    │   │   │   └── orders_producer.py
    │   │   ├── consumers/
    │   │   │   ├── orders_consumer.py
    │   │   │   └── fraud_engine.py
    │   │   └── loader.py
    │   │
    │   ├── generator/
    │   │   ├── batch_generator.py
    │   │   └── stream_generator.py
    │   │
    │   ├── utils/
    │   │   ├── config.py
    │   │   └── db_utils.py
    │
    ├── finpro_dbt/
    │   ├── models/
    │   │   ├── staging/
    │   │   ├── intermediate/
    │   │   └── marts/
    │   ├── seeds/
    │   ├── snapshots/
    │   ├── tests/
    │   ├── dbt_project.yml
    │   └── profiles.yml
    │
    ├── docker-compose.yml
    ├── .env
    └── README.md

---

## 🧰 Technologies Used

- **Programming Language:** Python 3.12  
- **Orchestration:** Apache Airflow 2.10  
- **Streaming Platform:** Kafka (Confluent)  
- **Database / Batch Storage:** PostgreSQL 14  
- **Cloud Storage:** Google Cloud Storage (GCS)  
- **Cloud Data Warehouse:** Google BigQuery  
- **Data Modeling:** dbt-core & dbt-bigquery  
- **Containerization:** Docker & Docker Compose  
- **Monitoring & Notifications:** Discord Webhooks  
- **Utilities:** Faker and custom Python generators  

---

## 🪂 Airflow DAGs

### `batch_raw_loads.py`

- Generates Users & Products via batch generator.  
- Loads into PostgreSQL tables `raw_users` and `raw_products`.  
- Ensures idempotent, repeatable runs using conflict logic.

### `bigquery_ingest.py`

- Extracts data from PostgreSQL raw tables.  
- Writes data to GCS.  
- Loads GCS files into BigQuery raw tables.

### `transformation_dbt.py`

- Executes dbt runs to build:
  - Staging tables (`stg_`)  
  - Intermediate dim/fact tables  
  - Marts (`mart_`)  

These DAGs can be scheduled or triggered manually for demonstration.

---

## 📊 Monitoring & Notifications

### Ingestion Monitoring

- Compares row counts between PostgreSQL source tables and BigQuery destination tables.  
- Logs comparisons into a dedicated monitoring structure named `ingestion_monitoring` table.  
- Marks status as **Consistent** or **Inconsistent** per table / run.

### Discord Webhook Notifications

Defined in `airflow/dags/utils/notif_callbacks.py`:

- DAG-level success notifications.  
- Task failure alerts with error context.  
- Data consistency notifications (row count comparisons).  
- Includes information such as DAG name, task name, execution date, and status.

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose installed.  
- A Google Cloud project with:
  - BigQuery enabled  
  - GCS bucket for loading data  
- Valid GCP credentials accessible inside the containers.  

### Basic Setup

1. Clone this repository:

       git clone <your-repo-url>
       cd finpro

2. Configure environment variables (e.g., `.env`, GCP credentials, Kafka settings).  

3. Ensure paths for:
   - GCP service account key  
   - dbt profiles  
   - Airflow configuration  
   are correctly mounted in `docker-compose.yml`.

---

## 🐳 Docker Usage

Build images and start all services:

    docker compose up --build

Stop services:

    docker compose down

Clean up containers and volumes (fresh start):

    docker compose down -v

Once Airflow is up, you can access the web UI at the configured host/port (for example `http://localhost:8080`, depending on your Docker setup).

---

## ⚙ Configuration

Key configuration files:

- `.env`  
  - Holds environment variables for local and containerized runs.  

- `docker-compose.yml`  
  - Defines services: Airflow, PostgreSQL, Kafka, Loader, etc.  
  - Manages volume mounts for configs, dbt, and credentials.  

- `src/utils/config.py`  
  - Application-level configuration mappings (e.g., DB connection settings).  

- `finpro_dbt/profiles.yml`  
  - dbt profile for connecting to BigQuery (project, dataset, credentials path).  

Ensure that:

- Airflow containers can read GCP credentials.  
- dbt has the correct profile and permissions.  
- Kafka broker addresses match between producer, consumer, and loader.

---

## 👤 Author & Acknowledgments

**Author:** Muhammad Rifqy Irfanto  
This project was developed as part of the **Purwadhika Data Engineering Bootcamp Final Project**.

It is intended both as:

- A practical demonstration of **end-to-end data engineering skills**, and  
- A portfolio piece showcasing experience with **batch & streaming pipelines**, **fraud detection**, **ELT**, **BigQuery**, and **dbt**.

If you have questions, suggestions, or feedback, feel free to reach out or open an issue in the repository.

---