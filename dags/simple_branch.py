from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from pendulum import today

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": today("UTC").add(days=-1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def predestine(**kwargs):
    kwargs["ti"].xcom_push(key="choice", value="foo")


def let_fates_decide(**kwargs):
    return kwargs["ti"].xcom_pull(key="choice")


with DAG(
    dag_id="simple_branch",
    start_date=today("UTC").add(days=-1),
    schedule="@once",
    default_args=default_args,
    catchup=False,
    tags=["core"],
) as dag:
    (
        PythonOperator(task_id="predestine", python_callable=predestine)
        >> BranchPythonOperator(
            task_id="let_fates_decide",
            python_callable=let_fates_decide,
        )
        >> [EmptyOperator(task_id="foo"), EmptyOperator(task_id="bar")]
    )
