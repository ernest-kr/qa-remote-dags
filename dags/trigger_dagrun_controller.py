from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from pendulum import today

"""This example illustrates the use of the TriggerDagRunOperator. There are 2
entities at work in this scenario:
1. The Controller DAG - the DAG that conditionally executes the trigger
2. The Target DAG - DAG being triggered (in trigger_dagrun_target.py)

"""

dag = DAG(
    dag_id="trigger_controller_dag",
    default_args={"owner": "airflow", "start_date": today("UTC").add(days=-2)},
    schedule=None,
    tags=["core"],
)


trigger = TriggerDagRunOperator(
    task_id="test_trigger_dagrun",
    trigger_dag_id="trigger_target_dag",
    reset_dag_run=True,
    wait_for_completion=True,
    conf={"message": "Hello World"},
    dag=dag,
)
