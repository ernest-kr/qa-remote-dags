from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from pendulum import today

docs = """
####Purpose
The purpose of this dag is to change UI behaviors in the graph view such as when you hover over a taskgroup with the mouse.\n
It also changes the color of the taskgroups.
####Expected Behavior
This dag has 14 dummy tasks that are all expected to succeed.
"""

with DAG(
    dag_id="taskgrp_colors",
    start_date=today("UTC").add(days=-2),
    schedule=None,
    doc_md=docs,
    tags=["core", "taskgroups"],
) as dag:
    # ui and ui_fgcolor both set colors for the taskgroup in the graph section of the UI
    # tooltip sets the string for the black popup window when you hover over the taskgroup in the ui.
    with TaskGroup(
        group_id="group1", ui_color="orange", ui_fgcolor="blue", tooltip="This is a popup window!"
    ) as group1:
        d0 = EmptyOperator(task_id="dummy0")
        d1 = EmptyOperator(task_id="dummy1")
        d0 >> d1
        for i in range(2, 7):
            d1 >> EmptyOperator(task_id=f"dummy{i}")

    # ui and ui_fgcolor both set colors for the taskgroup in the graph section of the UI
    # tooltip sets the string for the black popup window when you hover over the taskgroup in the ui.
    with TaskGroup(
        group_id="group2", ui_color="cyan", ui_fgcolor="magenta", tooltip="This is a popup window!"
    ) as group2:
        d0 = EmptyOperator(task_id="dummy8")
        d1 = EmptyOperator(task_id="dummy9")
        d0 >> d1
        for i in range(10, 15):
            d1 >> EmptyOperator(task_id=f"dummy{i}")


[group1, group2]
