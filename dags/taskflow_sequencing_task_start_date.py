from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from plugins.airflow_dag_introspection import assert_homomorphic
from plugins.api_utility import get_task_instances

docs = """
####Purpose
This dag tests that the sequencing outputs from one function to inputs of another function of the Taskflow API works correctly.\n
Specifically we're looking for problems in two ways:\n
 - Either the taskflow structure fails to match the structure created by the traditional interface (graph homomorphism)\n
 - Or the value is not as expected because the operations were performed in the wrong order (non-commutativity)\n
####Expected Behavior
This dag has 9 tasks all of which are expected to succeed. If any task fails then there is a problem with the Taskflow API.
"""

two_days = datetime.now() - timedelta(days=2)

default_args = {"owner": "airflow", "depends_on_past": True}

constant = 32


def dummy5():
    return constant * 3


def dummy6(**context):
    pulled = context["ti"].xcom_pull(task_ids="group1.dummy5")
    print(pulled)
    return pulled // 2


def dummy7(**context):
    pulled = context["ti"].xcom_pull(task_ids="group1.dummy6")
    print(pulled)
    assert pulled == 48
    return pulled


# Just a function made so the structure is the same
def dummy8(**context):
    pass


dag_id = "test_taskflow_with_task_start_date"
with DAG(
    dag_id=dag_id,
    schedule=None,
    start_date=two_days,
    default_args=default_args,
    max_active_tasks=1,
    doc_md=docs,
    tags=["core"],
) as dag:
    with TaskGroup(group_id="group1"):
        t5 = PythonOperator(
            task_id="dummy5",
            python_callable=dummy5,
        )

        t6 = PythonOperator(
            task_id="dummy6",
            python_callable=dummy6,
        )

        t7 = PythonOperator(
            task_id="dummy7",
            python_callable=dummy7,
        )

        t8 = PythonOperator(task_id="dummy8", python_callable=dummy8)

        t5 >> t6 >> t7 >> t8

    @task_group(group_id="group2")
    def dec_task_grouper():
        @task
        def dummy5(val):
            return val * 3 - 2

        @task
        def dummy6(val, **context):
            assert val == 94
            return val // 2 - 4

        @task
        def dummy7(val, **context):
            assert val == 43
            return val - 6

        @task
        def dummy8(val, **context):
            dag_id = context["dag"].dag_id
            run_id = context["run_id"]
            response = get_task_instances(dag_id, run_id)
            task_instances = response.json()["task_instances"]
            print("task_instances: ", task_instances)
            three_upstream = list(filter(lambda ti: "group2.dummy5" in ti.get("task_id"), task_instances))
            print("three_upstream: ", three_upstream)
            two_upstream = list(filter(lambda ti: "group2.dummy6" in ti.get("task_id"), task_instances))
            print("two_upstream: ", two_upstream)
            one_upstream = list(filter(lambda ti: "group2.dummy7" in ti.get("task_id"), task_instances))
            print("one_upstream: ", one_upstream)
            assert (
                three_upstream[0].get("start_date")
                < two_upstream[0].get("start_date")
                < one_upstream[0].get("start_date")
            )
            assert val == 37

        return dummy8(dummy7(dummy6(dummy5(constant))))

    tg2 = dec_task_grouper()

    assert_structure_is_equal = PythonOperator(
        task_id="assert_structure_is_equal",
        python_callable=assert_homomorphic,
        # function takes a list that's why there's 2 square brackets
        op_args=[["group1", "group2"]],
    )

[t8, tg2] >> assert_structure_is_equal
