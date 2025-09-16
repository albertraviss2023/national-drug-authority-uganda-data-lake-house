from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='example_dag',
    start_date=datetime(2025, 6, 10),
    schedule_interval=None,
) as dag:
    task = DummyOperator(task_id='dummy_task')
    