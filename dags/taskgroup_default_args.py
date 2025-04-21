from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from pendulum import today
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
The purpose of this dag is to check that you can pass custom default_args to task groups.\n
This dag achieves this by looking in the logs of tasks run in task groups for a string from a callback function assigned to different task groups.
####Expected Behavior
This dag has 18 tasks all of which are expected to succeed.\n
If any task fails then either something is wrong with the default args for taskgroups or something is wrong with on_success_callback or on_execute_callback.
"""


def the_callback(context):
    print("On a successful task run this function, 'the_callback', gets called")


def group_call1(context):
    print("On a successful task run within a task group this function, 'group_call1', gets called")


def group_call2(context):
    print("On a successful task run within a task group this function, 'group_call2', gets called")


args = {
    "start_date": today("UTC").add(days=-2),
    "schedule": None,
    "on_success_callback": the_callback,
}

group1_args = {"start_date": today("UTC").add(days=-3), "on_execute_callback": group_call1}

group2_args = {"start_date": today("UTC").add(days=-3), "on_execute_callback": group_call2}


def base_def_args():
    return 0


with DAG(
    dag_id="check_def_arg_pass_to_task_group",
    # schedule=None,
    schedule=None,
    start_date=today("UTC").add(days=-5),
    catchup=True,
    default_args=args,
    doc_md=docs,
    tags=["core", "taskgroups"],
) as dag:
    with TaskGroup(group_id="group1", default_args=group1_args) as group1:
        d0 = EmptyOperator(task_id="dummy0")
        d1 = EmptyOperator(task_id="dummy1")
        d0 >> d1
        for i in range(2, 7):
            d0 >> EmptyOperator(task_id=f"dummy{i}")

    with TaskGroup(group_id="group2", default_args=group2_args) as group2:
        d0 = EmptyOperator(task_id="dummy0")
        d1 = EmptyOperator(task_id="dummy1")
        d0 >> d1
        for i in range(2, 7):
            d0 >> EmptyOperator(task_id=f"dummy{i}")

    py15 = PythonOperator(
        task_id="check_default_args",
        python_callable=base_def_args,
    )

    logs_group1 = PythonOperator(
        task_id="check_logs_group1",
        python_callable=log_checker,
        op_args=["group1.dummy4", "'group_call1'", "'group_call2"],
    )

    logs_group2 = PythonOperator(
        task_id="check_logs_group2",
        python_callable=log_checker,
        op_args=["group2.dummy4", "'group_call2'", "'group_call1'"],
    )

    logs_base = PythonOperator(
        task_id="check_log_base",
        python_callable=log_checker,
        op_args=["check_default_args", "'the_callback'", "'called'"],
    )


[group1, group2] >> py15 >> logs_base >> [logs_group1, logs_group2]
