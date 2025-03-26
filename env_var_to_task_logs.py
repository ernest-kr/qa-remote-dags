import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Define the function that will print the environment variable
def print_env_var():
    # Access environment variable using os.getenv
    api_key = os.getenv("MY_API_KEY", "default_value")
    print(f"The value of MY_API_KEY is: {api_key}")
    os.environ["TEST"] = "this is different key"
    # Print it to the task logs
    print(f"The value of TEST is: {os.getenv('TEST')}")


# Define the DAG
dag = DAG(
    'print_env_var_in_logs',
    description='DAG to print environment variable in task logs',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Create the task using PythonOperator
print_env_var_task = PythonOperator(
    task_id='print_env_var_task',
    python_callable=print_env_var,
    dag=dag
)
