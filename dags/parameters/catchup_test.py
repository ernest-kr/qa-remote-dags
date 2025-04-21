from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from pendulum import today

default_args = {
    "owner": "airflow",
    "start_date": today("UTC").add(days=-1),
}
dag_name = "catchup_test"

with DAG(
    dag_name,
    default_args=default_args,
    schedule="@once",
    max_active_runs=1,
    catchup=False,
    tags=["core", "dagparams"],
) as dag:
    t1 = BashOperator(task_id="catchup", bash_command="date", dag=dag)
