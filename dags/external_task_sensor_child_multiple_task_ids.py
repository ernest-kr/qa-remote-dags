from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

docs = """
####Purpose
This dag is a simple dag whose only purpose is to be ran at a specific execution date so it can be checked by the ExternalTaskSensor.\n
The validity of the success of this dag comes from the ExternalTaskSensor checking the task_id's.
####Expected Behavior
All tasks expected to succeed.
"""

with DAG(
    dag_id="external_task_sensor_child_dag",
    start_date=datetime(2022, 3, 12, 3, 28, 0),
    schedule=timedelta(days=1),
    is_paused_upon_creation=False,
    doc_md=docs,
    tags=["core", "sensor"],
) as dag:
    d0 = EmptyOperator(task_id="child_dummy1")

    d2 = EmptyOperator(task_id="child_dummy2")

d0 >> d2
