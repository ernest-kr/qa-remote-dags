from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

## this DAG should how we can use context manager in setup and teardown

with DAG(
    dag_id="Setup_teardown_context_manager",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["setup_teardown"],
) as dag:
    ## way 1 tasks wrapped in setup and teardown task
    #
    root_setup = BashOperator(task_id="root_setup", bash_command="echo 'Hello from root_setup'")
    root_teardown = BashOperator(task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'")

    with root_teardown.as_teardown(setups=root_setup):
        root_normal = BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
        normal1 = BashOperator(task_id="normal1", bash_command="echo 'I am just a normal task'")
        normal2 = BashOperator(task_id="normal2", bash_command="echo 'I am just a normal task'")
        normal3 = BashOperator(task_id="normal3", bash_command="echo 'I am just a normal task'")
        normal4 = BashOperator(task_id="normal4", bash_command="echo 'I am just a normal task'")
        normal5 = BashOperator(task_id="normal5", bash_command="echo 'I am just a normal task'")
        root_normal >> [normal1 >> normal2] >> normal3 >> [normal4, normal5]

    ## way2

    # root_setup = BashOperator(task_id="root_setup", bash_command="echo 'Hello from root_setup'")
    # root_teardown = BashOperator(
    #     task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'"
    # ).as_teardown(setups=root_setup)
    #
    # with root_teardown:
    #     root_normal >> [normal1 >> normal2] >> normal3 >>[normal4,normal5]

    ## way3

    #
    # root_setup = BashOperator(task_id="root_setup", bash_command="echo 'Hello from root_setup'")
    # root_teardown = BashOperator(
    #     task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'"
    # ).as_teardown(setups=root_setup)
    #
    # with root_teardown as scope:
    #     scope.add_task(root_normal)
