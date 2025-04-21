from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from pendulum import today

default_args = {
    "owner": "airflow",
    "start_date": today("UTC").add(days=-1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag_name = "depends_on_past"

with DAG(
    dag_name,
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=5,
    tags=["core"],
) as dag:
    test1 = EmptyOperator(
        task_id="should_pass_1",
        max_active_tis_per_dag=10,
    )

    test2 = BashOperator(
        task_id="should_pass_2",
        bash_command="echo hi",
        depends_on_past=True,
    )

    test3 = BashOperator(
        task_id="should_pass_3",
        bash_command="echo hi",
    )

    test1 >> test2 >> test3
