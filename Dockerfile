FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER root

# Create log directories that Airflow expects at startup — must exist
# BEFORE Python logging dictConfig runs. These dirs need to be writable
# by the airflow user (UID set via AIRFLOW_UID).
RUN mkdir -p /opt/airflow/logs/dag_processor_manager /opt/airflow/logs/scheduler \
    && chmod -R 777 /opt/airflow/logs

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
