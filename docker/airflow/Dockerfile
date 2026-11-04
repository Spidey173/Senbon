FROM apache/airflow:2.9.3-python3.11

USER root

# Install system compilation dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set Python Path to root directory for shared imports
ENV PYTHONPATH="/opt/airflow:/opt/airflow/scripts:/opt/airflow/config"
ENV DBT_PROFILES_DIR="/opt/airflow/dbt"
ENV DBT_PROJECT_DIR="/opt/airflow/dbt"

