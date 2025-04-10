from datetime import timedelta

from airflow.models import DAG
from pendulum import today
from airflow.providers.standard.operators.python import PythonOperator
from werkzeug.exceptions import Unauthorized

args = {
    "owner": "airflow",
    "start_date": today('UTC').add(days=-2),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}
dag = DAG(
    dag_id="Python_operator_test_failure_dag",
    default_args=args,
    schedule=None,
    tags=["core"],
)


def run_this_func():
    print("hello")


def run_this_func2():
    print("hi")
    raise Unauthorized("401 error unauthorized to run this code")


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=run_this_func,
    )
    run_this_task2 = PythonOperator(task_id="run_this2", python_callable=run_this_func2)
    run_this_task3 = PythonOperator(task_id="run_this3", python_callable=run_this_func)


run_this_task >> run_this_task2 >> run_this_task3
