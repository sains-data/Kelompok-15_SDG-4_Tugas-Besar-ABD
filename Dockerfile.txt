FROM apache/airflow:2.8.1

USER root

# Install system dependencies (Java runtime is required for SparkSubmitOperator)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages required for the Spark and MinIO pipeline
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.1 \
    pyspark==3.4.1 \
    boto3==1.34.69 \
    pandas==2.0.3 \
    pyarrow==12.0.1 \
    openpyxl==3.1.2
