# from airflow.utils import edge_modifier
from datetime import datetime, timedelta
from textwrap import indent

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import cross_downstream
from airflow.sdk import chain
from airflow.utils.dot_renderer import render_dag

two_days = datetime.now() - timedelta(days=2)


@dag(dag_id="taskflow_cross_downstream", schedule=None, start_date=two_days, tags=["core"])
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
        def dummy00():
            return 0

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

        @task
        def dummy5(val):
            return val + 5

        @task
        def dummy6(val):
            return val + 6

        @task
        def dummy7(val):
            return val + 7

        d00 = dummy00()
        d0 = dummy0()
        d1 = dummy1(0)
        d2 = dummy2(0)
        d3 = dummy3(0)
        d4 = dummy4(0)
        d5 = dummy5(0)
        d6 = dummy6(0)
        d7 = dummy7(0)

        cross_downstream(from_tasks=[d00, d0, d1, d2, d3, d4], to_tasks=[d5, d6, d7])
        return d7

    @task_group(group_id="group2")
    def grouper2():
        @task
        def dummy00():
            return 0

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

        @task
        def dummy5(val):
            return val + 5

        @task
        def dummy6(val):
            return val + 6

        @task
        def dummy7(val):
            return val + 7

        d00 = dummy00()
        d0 = dummy0()
        d1 = dummy1(0)
        d2 = dummy2(0)
        d3 = dummy3(0)
        d4 = dummy4(0)
        d5 = dummy5(0)
        d6 = dummy6(0)
        d7 = dummy7(0)

        d00.set_downstream(d5)
        d00.set_downstream(d6)
        d00.set_downstream(d7)
        d0.set_downstream(d5)
        d0.set_downstream(d6)
        d0.set_downstream(d7)
        d1.set_downstream(d5)
        d1.set_downstream(d6)
        d1.set_downstream(d7)
        d2.set_downstream(d5)
        d2.set_downstream(d6)
        d2.set_downstream(d7)
        d3.set_downstream(d5)
        d3.set_downstream(d6)
        d3.set_downstream(d7)
        d4.set_downstream(d5)
        d4.set_downstream(d6)
        d4.set_downstream(d7)

        return d7

    tg1 = grouper1()
    tg2 = grouper2()

    # [tg1, tg2] >> assert_homomorphic(["group1", "group2"])
    chain([tg1, tg2], assert_homomorphic(["group1", "group2"]))


dag = task_grouper()
