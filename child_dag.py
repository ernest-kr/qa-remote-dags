from airflow import DAG
from airflow.operators.python import PythonOperator

def run_this_func2():
    print("hi")

with DAG(
    "dag_child",
    schedule=None,
    catchup=False,
) as dag:

    start_task = PythonOperator(task_id="run_this2", python_callable=run_this_func2)


    start_task
