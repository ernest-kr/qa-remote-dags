from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils import timezone
import time

# Define a simple function to sleep for 30 seconds
def sleep_task():
    print("Sleeping for 30 seconds...")
    time.sleep(30)
    print("Finished sleeping!")

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": timezone.utcnow() - timedelta(days=1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "simple_sleep_dag",
    default_args=default_args,
    description="A simple DAG that sleeps for 30 seconds",
) as dag:

    sleep_task_op = PythonOperator(
        task_id="sleep_for_30_seconds",
        python_callable=sleep_task,
    )

    sleep_task_op  # This sets the task in the DAG

