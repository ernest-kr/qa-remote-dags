from datetime import time

from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule
from pendulum import today
from plugins.airflow_dag_introspection import assert_the_task_states

docs = """
####Purpose
This dag tests the BranchDateTimeOperator to ensure that tasks within the timeframe set run and tasks outside of that timeframe are skipped.\n
It acheives this test by setting the start date of the tasks that are skipped to start one day earlier than set in the dag parameter 'start_date' so that the skipped tasks are always outside of the time range.\n
####Expected Behavior
This dag has 6 tasks 4 of the tasks are expected to succeed while 2 of the tasks are expected to be skipped.\n
This dag should pass.
"""

with DAG(
    dag_id="branch_date_time_operator",
    start_date=today("UTC").add(days=-2),
    schedule=None,
    max_active_runs=1,
    doc_md=docs,
    tags=["core"],
) as dag:
    d0 = EmptyOperator(task_id="dummy0", start_date=today("UTC").add(days=-3))
    d1 = EmptyOperator(task_id="dummy1", start_date=today("UTC").add(days=-3))

    d2 = EmptyOperator(task_id="dummy2")
    d3 = EmptyOperator(task_id="dummy3")

    bdto = BranchDateTimeOperator(
        task_id="branch_datetime",
        follow_task_ids_if_true=["dummy0", "dummy1"],
        follow_task_ids_if_false=["dummy2", "dummy3"],
        # target_upper and target_lower are time ranges for tasks to be in
        target_upper=time(hour=2, minute=33),
        target_lower=time(hour=0, minute=0, second=22),
    )

    py0 = PythonOperator(
        task_id="assert_the_states",
        python_callable=assert_the_task_states,
        op_args=[
            {
                "branch_datetime": "success",
                "dummy0": "skipped",
                "dummy1": "skipped",
                "dummy2": "success",
                "dummy3": "success",
            }
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

bdto >> [d0, d1, d2, d3] >> py0
