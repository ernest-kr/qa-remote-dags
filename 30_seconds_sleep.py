from airflow.models import DAG
from pendulum import today
from airflow.providers.standard.operators.python import PythonOperator

args = {
    "owner": "airflow",
    "start_date": today('UTC').add(days=-2),
}
dag = DAG(
    dag_id="Python_operator_test_failure_dag2",
    default_args=args,
    schedule=None,
    tags=["core"],
)


def run_this_func():
    print("hello")


def run_this_func2():
    print("hi")
    raise Exception("Failing dag intentionally")


with dag:
    run_this_task = PythonOperator(
        task_id="run_this",
        python_callable=run_this_func,
    )
    run_this_task2 = PythonOperator(task_id="run_this2", python_callable=run_this_func2)
    run_this_task3 = PythonOperator(task_id="run_this3", python_callable=run_this_func)


run_this_task >> run_this_task2 >> run_this_task3
