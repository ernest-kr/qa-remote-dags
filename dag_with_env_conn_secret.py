from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable


# Function to demonstrate environment variables, connections, and secrets
def access_env_conn_secret():
    # Access environment variable
    my_env_var = os.getenv("MY_ENV_VAR", "default_value")
    print(f"Environment Variable: {my_env_var}")

    # Fetch Airflow connection details
    conn_id = "my_gcp_connection"  # Change this based on your setup
    conn = BaseHook.get_connection(conn_id)
    print(f"Connection: {conn}")

    # Fetch secret from Airflow Variables
    my_secret = Variable.get("MY_SECRET_KEY", default_var="default_secret")
    print(f"Secret from Airflow Variable: {my_secret}")


# Define DAG default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 4, 1),
    "retries": 1,
}

# Create the DAG
dag = DAG(
    "dag_with_env_conn_secret",
    default_args=default_args,
    description="A DAG that uses environment variables, connections, and secrets",
    schedule=None,  # Manual trigger
)

# Define the PythonOperator
task = PythonOperator(
    task_id="use_env_conn_secret",
    python_callable=access_env_conn_secret,
    dag=dag,
)
