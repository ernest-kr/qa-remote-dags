from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep0():
    print("short sleep for 30 sec")
    sleep(20 * 1)


def short_sleep1():
    print("short sleep for 30 sec")
    sleep(20 * 1)
    raise Exception("Failing dag intentionally")


def passing_task():
    sleep(5 * 1)


with DAG(
    "20_seconds_sleep_two_tasks_failure_dag",
    start_date=datetime(2022, 8, 10),
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    max_active_tasks=1000,
    catchup=False,
) as dag:
    task_dag_map = {
        "0": "lineage-combine-postgres",
        "1": "demo_trigger_rules",
        "2": "dynamic_postgres_demo",
    }
    task1 = PythonOperator(task_id="20_seconds_sleep_task1", python_callable=short_sleep0)
    task2 = PythonOperator(task_id="20_seconds_sleep_task2", python_callable=short_sleep1)
    task3 = PythonOperator(task_id="20_seconds_sleep_task3", python_callable=passing_task)

    task1 >> task2 >> task3
