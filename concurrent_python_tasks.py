from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_this_func():
    print("hello")

with DAG(
    'concurrent_python_tasks',
    default_args=default_args,
    schedule=timedelta(minutes=2),
    catchup=False,
    tags=['bash', 'concurrent'],
) as dag:
    tasks = []
    for i in range(12):
        sleep_time = random.randint(10, 60)  # Sleep between 10 and 60 seconds
        # task = BashOperator(
        #     task_id=f'bash_task_{i}',
        #     bash_command=f'echo "Starting task {i} and sleeping for {sleep_time} seconds"; sleep {sleep_time}; echo "Task {i} completed"',
        # )
        task = PythonOperator(task_id=f"run_this_{i}", python_callable=run_this_func)
        tasks.append(task)

    # No dependencies, so all tasks run concurrently
