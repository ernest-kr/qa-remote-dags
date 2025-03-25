from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

def task_3_logic():
    """Simulate task 3's behavior with a 50% chance of failure."""
    #if random.random() < 0.5:
    #raise Exception("Task 3 failed!")
    print("Task 3 completed successfully.")

def task_4_logic():
    """Simulate task 4's behavior."""
    choice = random.choice([+5, -5])
    print("Task 4 completed successfully.")

def task_5_logic():
    """Simulate task 4's behavior."""
    choice = random.choice([+7, -7])
    choice = 0
    sleep_time = 300
    print(f"sleep time is {sleep_time}")
    time.sleep(sleep_time-choice)
    print("Task 4 completed successfully.")

# Define DAG
dag = DAG(
    'example_complex_dag',
    default_args=default_args,
    description='A DAG with complex task dependencies and logic.',
    schedule_interval='*/10 * * * *',  # Every 5 minutes between 8 pm and 10 pm EST
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=['example'],
)

# Define tasks
task_1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 20',
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 30',
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3_logic,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='task_4',
    python_callable=task_4_logic,
    dag=dag,
)

task_5 = PythonOperator(
    task_id='task_5',
    python_callable=task_5_logic,
    dag=dag,
)

# Set dependencies
task_1 >> task_4
task_2 >> task_4
task_3 >> task_4
task_5 >> task_4
