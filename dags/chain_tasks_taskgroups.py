# testing regular tasks, task groups and labels with chain
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, chain
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

two_days = datetime.now() - timedelta(days=2)

default_args = {"owner": "airflow", "depends_on_past": True}

with DAG(
    dag_id="chain_tasks_and_task_groups",
    schedule=None,
    start_date=two_days,
    default_args=default_args,
    tags=["core"],
) as dag:
    t0 = BashOperator(task_id="sleep_3_seconds", bash_command="sleep 3")

    with TaskGroup(group_id="group1") as taskgroup1:
        t1 = EmptyOperator(task_id="dummmy1")
        t2 = EmptyOperator(task_id="dummy2")
        t3 = EmptyOperator(task_id="dummy3")

    t7 = BashOperator(task_id="bash_echo", bash_command="echo continue..")

    t8 = EmptyOperator(task_id="dummy4")
    t9 = EmptyOperator(task_id="dummy5")
    t10 = EmptyOperator(task_id="dummy6")
    t11 = EmptyOperator(task_id="dummy7")
    t12 = EmptyOperator(task_id="dummy8")


# chaining tasks and task groups with labels for regular tasks and grouped tasks
chain(
    t0,
    [Label("branching to group tasks"), Label("stuff")],
    taskgroup1,
    t7,
    [Label("branch one"), Label("branch two"), Label("branch three"), Label("branch four"), Label("branch five")],
    [t8, t9, t10, t11, t12],
)
