from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_function():
    print("Running on KubernetesExecutor")

def start_function():
    print("This is the result of task start")

with DAG('test_kubernetes_executor',
         default_args={'owner': 'airflow', 'start_date': datetime(2025, 3, 26)},
         catchup=False) as dag:

    run_test = PythonOperator(
        task_id='run_test',
        python_callable=start_function
    )

    run_test = PythonOperator(
        task_id='run_test',
        python_callable=test_function
    )

    start >> run_test
