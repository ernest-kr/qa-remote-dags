from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="bash_wait_dag",
    schedule=None,
    tags=["bash", "example"],
    timeout=60,
) as dag:
    
    wait_task = BashOperator(
        task_id="wait_75_seconds",
        bash_command="sleep 75"
    )
    
    wait_task
