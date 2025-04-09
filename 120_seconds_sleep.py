from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep():
    print("short sleep for 120 sec")
    sleep(60 * 2)


with DAG(
    "120_seconds_sleep",
    start_date=datetime(2022, 8, 10),
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    max_active_tasks=1000,
    catchup=False,
    schedule=None,
) as dag:
    task_dag_map = {
        "0": "lineage-combine-postgres",
        "1": "demo_trigger_rules",
        "2": "dynamic_postgres_demo",
    }
    task1 = PythonOperator(task_id="120_seconds_sleep_task", python_callable=short_sleep)

    task1
