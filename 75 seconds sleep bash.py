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
        bash_command="sleep 700"
    )
    
    wait_task
