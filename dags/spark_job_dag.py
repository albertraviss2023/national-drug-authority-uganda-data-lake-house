from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='spark_job_dag',
    start_date=datetime(2025, 6, 12),
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:

    # Define the SparkSubmitOperator task
    spark_task = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/spark_jobs/job.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',  # Correct master URL
            'spark.total.executor.cores': '1',
            'spark.executor.memory': '1g',
        },
        name='arrow-spark',
        verbose=True,  # Enable verbose logging
    )

    spark_task  # Single task, no dependencies
