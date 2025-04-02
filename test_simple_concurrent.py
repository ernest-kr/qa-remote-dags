from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_simple_concurrent',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False,
    tags=['bash', 'concurrent'],
) as dag:
    tasks = []
    for i in range(12):
        sleep_time = random.randint(10, 60)  # Sleep between 10 and 60 seconds
        task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f'echo "Starting task {i} and sleeping for 1 seconds"; sleep 1; echo "Task {i} completed"',
        )
        tasks.append(task)
