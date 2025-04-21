from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState
from plugins.airflow_dag_introspection import assert_the_task_states

docs = """
####Purpose
This dag tests that the ExternalTaskSensor is able to sense multiple tasks with the parameter 'external_task_ids=['list', 'of', 'task', 'ids']'.\n
The ExternalTaskSensor senses only task_ids with a specific execution date. by default it searches for task_ids with the same execution date as that of the dag with the sensor.\n
If there is a different execution date than that of the the dag that has the sensor then either the execution_delta or the execution_delta_fn needs to be set.\n
####Expected Behahvior
This dag has 1 tasks that are both expected to succeed.\n
The first task senses the completion of the tasks child_dummy1 and child_dummy2 in the dag with a python file of multiple_tasks_ids_retake_child.py and with a dag_id of external_task_sensor_child_dag .\n
"""


def ex_date(dt):
    new_dt = dt - timedelta(days=1)
    return new_dt


with DAG(
    dag_id="external_task_sensor_parent",
    start_date=datetime(2022, 3, 12, 3, 35, 0),
    schedule=timedelta(days=1),
    #  is_paused_upon_creation=False,
    doc_md=docs,
    tags=["core", "sensor"],
) as dag:
    ets1 = ExternalTaskSensor(
        task_id="check_ids",
        external_dag_id="external_task_sensor_child_dag",
        external_task_ids=["child_dummy1", "child_dummy2"],
        allowed_states=[TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED],
        failed_states=[
            TaskInstanceState.FAILED,
            TaskInstanceState.REMOVED,
        ],
        execution_delta=timedelta(minutes=7),
        # execution_date_fn=ex_date,
        check_existence=True,
        poke_interval=20.0,
        timeout=420.0,
    )

    py1 = PythonOperator(
        task_id="check_state_of_sensor",
        python_callable=assert_the_task_states,
        op_args=[{"check_ids": "success"}],
    )

ets1 >> py1
