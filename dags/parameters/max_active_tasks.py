from itertools import product

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.api_utility import get_task_instances

docs = """
####Purpose
This dag tests the dag parameter 'max_active_tasks' which sets the maximum amount of concurrent tasks in the dag.
####Expected Behavior
This dag has 3 BashOperator tasks that sleep for 30 seconds each and 1 PythonOperator task.\n
The PythonOperator task compares the starts and end dates between the 3 BashOperator to ensure a BashOperator task completed separately from the 2 that were running concurrently.
"""


def get_tis(**context):
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    response = get_task_instances(dag_id, run_id)
    task_instances = response.json()["task_instances"]
    three_tasks = list(filter(lambda ti: "sleep30" in ti.get("task_id"), task_instances))
    found = None
    for one_task, other_task in product(three_tasks, three_tasks):
        if one_task.get("start_date") > other_task.get("end_date"):
            found = (one_task, other_task)

    if found:
        print(f"{found[0]} started after {found[1]}")
    else:
        raise Exception("None of these tasks waited for the other two and the concurrency is set to 2")


with DAG(
    dag_id="max_active_tasks",
    start_date=today("UTC").add(days=-2),
    schedule=None,
    max_active_tasks=2,
    doc_md=docs,
    tags=["dagparams"],
) as dag:
    b0 = BashOperator(task_id="sleep30sec1", bash_command="sleep 30")
    b1 = BashOperator(task_id="sleep30sec2", bash_command="sleep 30")
    d0 = BashOperator(task_id="sleep30sec3", bash_command="sleep 30")

    py0 = PythonOperator(
        task_id="get_task_instances",
        python_callable=get_tis,
    )

[b0, b1, d0] >> py0
