from datetime import datetime, timedelta
from textwrap import indent

from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, chain
from airflow.utils.dag_edges import dag_edges
from airflow.utils.dot_renderer import render_dag
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

two_days = datetime.now() - timedelta(days=2)


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


def assert_labels_appear(assertions, **context):
    """
    the labels defined in the task dependencies should be the same as the labels pulled from dag_edges(context['dag'])
    """
    dag = context["dag"]
    edges = dag_edges(dag)
    found_labels = []
    for i in edges:
        if "label" in i:
            found_labels.append((i["source_id"], i["target_id"], i["label"]))

    assert assertions == found_labels


with DAG(
    dag_id="check_edge_labels",
    schedule=None,
    start_date=two_days,
    tags=["core"],
) as dag:
    with TaskGroup(group_id="group1") as taskgroup1:
        t0 = EmptyOperator(task_id="dummy0")
        t1 = EmptyOperator(task_id="dummy1")
        t2 = EmptyOperator(task_id="dummy2")
        t3 = EmptyOperator(task_id="dummy3")
        t4 = EmptyOperator(task_id="dummy4")
        chain(t0, [Label("branch one"), Label("branch two"), Label("branch three")], [t1, t2, t3], t4)

    with TaskGroup(group_id="group2") as taskgroup2:
        t10 = EmptyOperator(task_id="dummy0")
        t11 = EmptyOperator(task_id="dummy1")
        t12 = EmptyOperator(task_id="dummy2")
        t13 = EmptyOperator(task_id="dummy3")
        t14 = EmptyOperator(task_id="dummy4")
        t10.set_downstream(t11, edge_modifier=Label("branch one"))
        t10.set_downstream(t12, edge_modifier=Label("branch two")),
        t10.set_downstream(t13, edge_modifier=Label("branch three"))
        t11.set_downstream(t14)
        t12.set_downstream(t14)
        t13.set_downstream(t14)

    t14 = PythonOperator(
        task_id="matts_assert",
        python_callable=assert_labels_appear,
        op_args=[
            [
                (t0.task_id, t1.task_id, "branch one"),
                (t0.task_id, t2.task_id, "branch two"),
                (t0.task_id, t3.task_id, "branch three"),
                (t10.task_id, t11.task_id, "branch one"),
                (t10.task_id, t12.task_id, "branch two"),
                (t10.task_id, t13.task_id, "branch three"),
            ]
        ],
    )

    [taskgroup1, taskgroup2] >> assert_homomorphic(["group1", "group2"]) >> t14
