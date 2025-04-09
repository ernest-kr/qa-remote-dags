from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep():
    print("short sleep for 30 sec")
    sleep(30 * 1)
    raise Exception("Failing dag intentionally")


with DAG(
    "30_seconds_sleep_failure_dag_with_retries",
    start_date=datetime(2022, 8, 10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    max_active_tasks=1000,
    catchup=False,
    schedule=None,
) as dag:
    task_dag_map = {
        "0": "lineage-combine-postgres",
        "1": "demo_trigger_rules",
        "2": "dynamic_postgres_demo",
    }
    task1 = PythonOperator(task_id="30_seconds_sleep_task", python_callable=short_sleep)

    task1
