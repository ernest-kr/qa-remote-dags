import pendulum
from airflow.decorators import setup, task, teardown
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="setup_group_teardown_group_2", start_date=pendulum.now(), tags=["setup_teardown"]):
    with TaskGroup("group_1") as g1:

        @setup
        def setup_1():
            print("this is setup1")

        @setup
        def setup_2():
            print("this is setup2")

        @teardown
        def teardown_0():
            print("this is teardown0")

        s1 = setup_1()
        s2 = setup_2()
        t0 = teardown_0()
        s2 >> t0

    with TaskGroup("group_2") as g2:

        @teardown
        def teardown_1():
            print("this is teardown1")

        @teardown
        def teardown_2():
            print("this is teardown2")

        t1 = teardown_1()
        t2 = teardown_2()

    @task
    def work():
        print("this is work")

    w1 = work()
    g1 >> w1 >> g2
    t1.as_teardown(setups=s1)
    t2.as_teardown(setups=s2)
