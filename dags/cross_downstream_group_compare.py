from datetime import datetime, timedelta
from textwrap import indent

from airflow.decorators import task
from airflow.models.baseoperator import chain, cross_downstream
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.utils.dot_renderer import render_dag
from airflow.utils.task_group import TaskGroup

two_days = datetime.now() - timedelta(days=2)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
}


@task
def assert_homomorphic(task_group_names, **context):
    """
    The structure of all of the task groups above should be the same
    """
    # get the dag in dot notation, focus only on its edges
    dag = context["dag"]
    # gives string which represents whole dag structure
    graph = render_dag(dag)
    print("Whole DAG:")
    print(indent(str(graph), "    "))
    lines = list(filter(lambda x: "->" in x, str(graph).split("\n")))

    # bin them by task group, then remove the group names
    group_strings = []
    # removes everything thats not a task name
    for name in task_group_names:
        print(name)
        relevant_lines = filter(lambda x: name in x, lines)
        normalized_lines = (x.strip().replace(name, "") for x in sorted(relevant_lines))
        edges_str = "\n".join(normalized_lines)
        group_strings.append(edges_str)
        print(indent(edges_str, "    "))

    # these should be identical
    for xgroup, ygroup in zip(group_strings, group_strings[1:]):
        assert xgroup == ygroup


with DAG(
    dag_id="cross_downstream_group_compare",
    schedule=None,
    start_date=two_days,
    default_args=default_args,
    tags=["core"],
) as dag:
    with TaskGroup(group_id="group1") as tg1:
        t1 = EmptyOperator(task_id="dummy1")
        t2 = EmptyOperator(task_id="dummy2")
        t3 = EmptyOperator(task_id="dummy3")
        t4 = EmptyOperator(task_id="dummy4")
        t5 = EmptyOperator(task_id="dummy5")
        t6 = EmptyOperator(task_id="dummy6")
        t7 = EmptyOperator(task_id="dummy7")

        @task
        def dummy0():
            return 0

        d0 = dummy0()

        cross_downstream(from_tasks=[d0, t1, t2, t3, t4], to_tasks=[t5, t6, t7])

    with TaskGroup(group_id="group2") as tg2:
        t1 = EmptyOperator(task_id="dummy1")
        t2 = EmptyOperator(task_id="dummy2")
        t3 = EmptyOperator(task_id="dummy3")
        t4 = EmptyOperator(task_id="dummy4")
        t5 = EmptyOperator(task_id="dummy5")
        t6 = EmptyOperator(task_id="dummy6")
        t7 = EmptyOperator(task_id="dummy7")

        @task
        def dummy0():
            return 0

        d0 = dummy0()

        d0.set_downstream(t5)
        d0.set_downstream(t6)
        d0.set_downstream(t7)
        t1.set_downstream(t5)
        t1.set_downstream(t6)
        t1.set_downstream(t7)
        t2.set_downstream(t5)
        t2.set_downstream(t6)
        t2.set_downstream(t7)
        t3.set_downstream(t5)
        t3.set_downstream(t6)
        t3.set_downstream(t7)
        t4.set_downstream(t5)
        t4.set_downstream(t6)
        t4.set_downstream(t7)


chain([tg1, tg2], assert_homomorphic(["group1", "group2"]))
