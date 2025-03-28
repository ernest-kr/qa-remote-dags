from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    "dag_parent",
    schedule=None,
    catchup=False,
) as dag:

    trigger_child_dag = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="dag_child",  # Name of the DAG to trigger
        wait_for_completion=False,   # Set to True if you want to wait until the child DAG completes
    )

    trigger_child_dag
