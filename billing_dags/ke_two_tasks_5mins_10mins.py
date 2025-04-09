from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep():
    print("short sleep for 5 Mins")
    sleep(300 * 1)


def short_sleep1():
    print("short sleep for 5 Mins")
    sleep(600 * 1)


with DAG(
    "ke_two_tasks_5mins_10mins",
    start_date=datetime(2022, 8, 10),
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    max_active_tasks=1000,
    catchup=False,
) as dag:
    task_dag_map = {
        "0": "lineage-combine-postgres",
        "1": "demo_trigger_rules",
        "2": "dynamic_postgres_demo",
    }
    task2 = PythonOperator(task_id="sleep_5mins", python_callable=short_sleep)
    task1 = PythonOperator(task_id="sleep_10mins", python_callable=short_sleep1)

    [task1, task2]
