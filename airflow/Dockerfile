FROM apache/airflow:2.8.1

# Switch to root to install system dependencies
USER root

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make

# Install gosu
RUN apt-get install -y gosu

# Switch back to airflow user
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-mongo \
    apache-airflow[amazon] \
    apache-airflow-providers-slack \
    apache-airflow[statsd] \
    pandas