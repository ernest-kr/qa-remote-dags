from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="setup_teardown_nested",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["setup_teardown"],
) as dag:
    root_setup = BashOperator(task_id="root_setup", bash_command="echo 'Hello from root_setup'").as_setup()
    normal = BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
    root_teardown = BashOperator(task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'").as_teardown(
        setups=root_setup
    )
    root_setup >> normal >> root_teardown
    with TaskGroup("section_1") as section_1:
        section_1_setup = BashOperator(
            task_id="taskgroup_setup", bash_command="echo 'Hello from taskgroup_setup'"
        ).as_setup()
        section1_normal = BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
        section_1_teardown = BashOperator(
            task_id="taskgroup_teardown", bash_command="echo 'Hello from taskgroup_teardown'"
        ).as_teardown(setups=section_1_setup)
        section_1_setup >> section1_normal >> section_1_teardown

    with TaskGroup("section_2") as section_2:
        section_2_setup = BashOperator(
            task_id="section_2_setup", bash_command="echo 'Hello from outer_taskgroup_setup'"
        ).as_setup()
        section_2_task = BashOperator(task_id="section_2_task", bash_command="echo test")
        section_2_teardown = BashOperator(
            task_id="section_2_teardown", bash_command="echo 'Goodbye from section_2_teardown'"
        ).as_teardown(setups=section_2_setup)
        section_2_setup >> section_2_task >> section_2_teardown
        with TaskGroup("section_2_inner") as section_2_inner:
            section_2_inner_setup = BashOperator(
                task_id="section_2_inner_setup", bash_command="echo 'Hello from inner_taskgroup_setup'"
            ).as_setup()
            section_2_inner_task = BashOperator(
                task_id="section_2_inner_task", bash_command="echo 'Hello from inner_taskgroup_normal'"
            )
            section_2_inner_teardown = BashOperator(
                task_id="section_2_inner_teardown", bash_command="echo 'Goodbye from section_2_inner_teardown'"
            ).as_teardown(setups=section_2_inner_setup)
            section_2_inner_setup >> section_2_inner_task >> section_2_inner_teardown

    normal >> section_1 >> section_2
