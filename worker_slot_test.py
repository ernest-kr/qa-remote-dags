from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,  # No retries for testing
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id='worker_slot_test',
    default_args=default_args,
    schedule_interval=None,  # Run manually for testing
    catchup=False,
    tags=['testing', 'worker_slots'],
) as dag:

    # Create multiple tasks that consume worker slots
    tasks = [
        BashOperator(
            task_id=f'sleep_task_{i}',
            bash_command=f'sleep 300 && echo "Task {i} completed"', #sleep for an increasing amount of time.
        )
        for i in range(10)  # Adjust the number of tasks to test your worker slots
    ]

    # Define task dependencies (run them in parallel)
    tasks
