FROM apache/airflow:3.0.0-python3.12

USER root

# Install system packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk \
        python3-pip \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/airflow/db /opt/airflow/logs/dag_processor_manager \
    && chmod -R 777 /opt/airflow/db /opt/airflow/logs

USER airflow

# Install Airflow with extras and required Python packages
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver \
    'apache-airflow[postgres,password]==3.0.0' \
    'apache-airflow-providers-ssh==3.12.0' \
    'apache-airflow-providers-apache-spark' \
    'kafka-python' \
    'requests' \
    'dbt-core' \
    'dbt-snowflake' \
    'hdfs' \
    'protobuf==4.21.12' \
    'proto-plus==1.22.3'
