FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements-airflow.txt