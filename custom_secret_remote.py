from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def fetch_secret():
    val = Variable.get("test-remote-variable3")  # Will look for `airflow/test-variable` in AWS
    print(f"Fetched secret value: {val}")
    if val != "super-secret-value":
        raise ValueError("Secret value mismatch!")

with DAG(
    "test_custom_secret_remote",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="check_secret",
        python_callable=fetch_secret
    )
