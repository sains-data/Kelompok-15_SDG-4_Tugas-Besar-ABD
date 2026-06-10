from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tubes_abd_medallion_pipeline',
    default_args=default_args,
    description='Pipeline ETL Medallion Architecture (Bronze -> Silver -> Gold)',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['tubes_abd', 'medallion']
)

# Task 1: XLSX to CSV (Bronze Layer)
bronze_task = BashOperator(
    task_id='process_bronze_layer',
    bash_command='python /opt/airflow/jobs/convert_xlsx_to_csv.py',
    dag=dag
)

# Task 2: Bronze to Silver (Data Cleaning & Parquet Conversion)
silver_task = SparkSubmitOperator(
    task_id='process_silver_layer',
    application='/opt/airflow/jobs/phase5_silver.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
    verbose=False,
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client"
    },
    dag=dag
)

# Task 3: Silver to Gold (Data Aggregation & Analytics)
gold_task = SparkSubmitOperator(
    task_id='process_gold_layer',
    application='/opt/airflow/jobs/phase6_gold.py',
    conn_id='spark_default',
    packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0',
    verbose=False,
    conf={
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client"
    },
    dag=dag
)

# Set dependencies
bronze_task >> silver_task >> gold_task
