from datetime import timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator


def short_sleep():
    print("short sleep for 5 sec")
    sleep(5 * 1)


with DAG(
    "5_seconds_sleep_dag",
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
    task1 = PythonOperator(task_id="5_seconds_sleep_task", python_callable=short_sleep)

    task1
