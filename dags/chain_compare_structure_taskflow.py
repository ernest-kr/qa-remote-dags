from datetime import datetime, timedelta
from textwrap import indent

from airflow.decorators import dag, task, task_group
from airflow.sdk import chain
from airflow.utils.dot_renderer import render_dag
from airflow.utils.edgemodifier import Label

two_days = datetime.now() - timedelta(days=2)


@dag(dag_id="compare_structure_taskflow", schedule=None, start_date=two_days, tags=["core"])
def task_grouper():
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

    @task_group(group_id="group1")
    def grouper1():
        @task
        def dummy0():
            return 0

        @task
        def dummy1(val):
            return val + 1

        @task
        def dummy2(val):
            return val + 2

        @task
        def dummy3(val):
            return val + 3

        @task
        def dummy4(val):
            return val + 4

        d0 = dummy0()
        d1 = dummy1(0)
        d2 = dummy2(0)
        d3 = dummy3(0)
        d4 = dummy4(0)
        # chain(dummy0(), [Label("branch one"), Label("branch two"), Label("branch three")], [dummy1(), dummy2(), dummy3()], dummy4())
        chain(d0, [Label("branch one"), Label("branch two"), Label("branch three")], [d1, d2, d3], d4)
        return d4

    @task_group(group_id="group2")
    def grouper2():
        @task
        def dummy0():
            return 0

        @task
        def dummy1(val):
            return val + 1

        @task
        def dummy2(val):
            return val + 2

        @task
        def dummy3(val):
            return val + 3

        @task
        def dummy4(val):
            return val + 4

        d0 = dummy0()
        d1 = dummy1(1)
        d2 = dummy2(1)
        d3 = dummy3(1)
        d4 = dummy4(1)

        d0.set_downstream(d1, edge_modifier=Label("branch one"))
        d0.set_downstream(d2, edge_modifier=Label("branch two"))
        d0.set_downstream(d3, edge_modifier=Label("branch three"))
        d1.set_downstream(d4)
        d2.set_downstream(d4)
        d3.set_downstream(d4)

        # return dummy4(dummy3(dummy2(dummy1(dummy0()))))
        # return chain(d0, [d1, d2, d3], d4)
        return d4
        # if you need to grab something before you need to return the first task to grab a handle on the first

    tg1 = grouper1()
    tg2 = grouper2()
    # tg2_end, tg2start = grouper2()

    # EmptyOperator(task_id="start") >> tg2start
    # [tg2_start,tg2_end, tg1] >> assert_homomorphic(["group1", "group2"])
    [tg1, tg2] >> assert_homomorphic(["group1", "group2"])


dag = task_grouper()
