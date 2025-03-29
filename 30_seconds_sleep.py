from airflow.models import DAG
from airflow.operators.python import PythonOperator
from time import sleep
from datetime import datetime, timedelta

args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(seconds=5)
}
dag = DAG(
    dag_id="Python_operator_test",
    default_args=args,
    schedule=None,
    tags=["core"],
)


def run_this_func():
    print("hello")


def run_this_func2():
    print("hi")
    raise Exception("Failed Intentionally")
    


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=run_this_func,
    )
    run_this_task2 = PythonOperator(task_id="run_this2", python_callable=run_this_func2)
    run_this_task3 = PythonOperator(task_id="run_this3", python_callable=run_this_func)


run_this_task >> run_this_task2 >> run_this_task3
