# Python operator positive test
# testing to pass with True/False and return text message

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today

args = {
    "owner": "airflow",
    "start_date": today("UTC").add(days=-2),
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


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=run_this_func,
    )
    run_this_task2 = PythonOperator(task_id="run_this2", python_callable=run_this_func2)
    run_this_task3 = PythonOperator(task_id="run_this3", python_callable=run_this_func)


run_this_task >> run_this_task2 >> run_this_task3
