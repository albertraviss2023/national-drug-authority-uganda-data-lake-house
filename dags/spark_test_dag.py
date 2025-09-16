from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='spark_test_dag',
    default_args=default_args,
    schedule_interval=None,  # Run manually for testing
    catchup=False,
)

spark_job = SparkSubmitOperator(
    task_id='word_count_job',
    conn_id='spark_default',  # Reference the connection ID
    application='/opt/spark_jobs/wordcount.py',  # Path to your PySpark script
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    name='word_count_job',
    dag=dag,
)

spark_job
