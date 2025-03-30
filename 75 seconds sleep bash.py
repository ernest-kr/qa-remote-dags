from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="bash_wait_dag",
    schedule=None,
    tags=["bash", "example"],
) as dag:
    
    wait_task = BashOperator(
        task_id="wait_75_seconds",
        bash_command="sleep 75",
        execution_timeout=timedelta(seconds=60),
        timeout=130,
        retries=1,
    )
    
    wait_task
