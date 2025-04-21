import os
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
This dag is testing that the owner parameter in the default args dictionary, which is also a dag parameter, shows up correctly.
####Expected Behavior
This dag has 2 tasks that are both expected to succeed. If either one or both tasks fail there is something wrong with default args.\n
The first task is python function has the 'pass' statement to make sure it always runs successfully.\n
The second task checks the logs of the first task to make sure the owner default arg key: value is assigned correctly.
"""

# every default arg you can enable. these are passed down to every operator in the dag
default_args_tut_example = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# 'priority_weight' have the tasks race
# set default to 1 with 5 tasks with all priority_weight: 1, and with another 5 with weighted priority of 2.
# last task with higher priority should've finished first.


def easy_return():
    print(f"AIRFLOW_CTX_DAG_OWNER={os.environ.get('AIRFLOW_CTX_DAG_OWNER')}")


default_args = {"owner": "airflow"}

with DAG(
    dag_id="default_args_owner",
    start_date=today("UTC").add(days=-1),
    schedule=None,
    default_args=default_args,
    doc_md=docs,
    tags=["dagparams"],
) as dag:
    py0 = PythonOperator(task_id="dummy1", python_callable=easy_return)

    py1 = PythonOperator(
        task_id="check_for_owner_in_def_args",
        python_callable=log_checker,
        op_args=[
            "dummy1",
            "AIRFLOW_CTX_DAG_OWNER=airflow",
            "AIRFLOW_CTX_DAG_OWNER='astro'",
        ],
    )
py0 >> py1
