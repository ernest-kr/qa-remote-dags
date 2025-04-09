from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep():
    print("short sleep for 5 Mins")
    sleep(30 * 1)
    raise Exception("Intentional failure")


with DAG(
    "ke_two_sequential_tasks",
    start_date=datetime(2022, 8, 10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=5)},
    max_active_tasks=1000,
    catchup=False,
) as dag:
    task_dag_map = {
        "0": "lineage-combine-postgres",
        "1": "demo_trigger_rules",
        "2": "dynamic_postgres_demo",
    }
    task2 = PythonOperator(task_id="5mins_sleep_2", python_callable=short_sleep)
    task1 = PythonOperator(task_id="5mins_sleep_1", python_callable=short_sleep)

    task1 >> task2
