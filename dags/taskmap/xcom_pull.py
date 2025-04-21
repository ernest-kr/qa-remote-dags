from datetime import datetime

from airflow.models.taskinstance import TaskInstance
from airflow.sdk import DAG

with DAG(
    dag_id="taskmap_xcom_pull",
    tags=["AIP_42", "taskmap"],
    start_date=datetime(1970, 1, 1),
    schedule=None,
) as dag:

    @dag.task
    def foo():
        return "foo"

    @dag.task
    def identity(thing):
        return thing

    @dag.task
    def uppercase(thing):
        return thing.upper()

    @dag.task(multiple_outputs=True)
    def two_parts(thing):
        return {"first": thing[0:3], "rest": thing[3:]}

    @dag.task
    def xcom_pull(**context):
        ti: TaskInstance = context["ti"]

        for x in [
            ti.xcom_pull(task_ids="identity"),
            # <airflow.models.taskinstance._LazyXComAccess object at 0x7f5d39f92430>
            ti.xcom_pull(task_ids="foo"),
            # foo
            ti.xcom_pull(task_ids="bar"),
            # []
            ti.xcom_pull(task_ids=["identity", "foo", "bar", "uppercase", "two_parts"]),
            # [2, 3, 1, 'foo', [], 'A', 'B', 'C', {'first': 'ips', 'rest': 'um'}, {'first': 'lor', 'rest': 'em'}, {'first': 'dol', 'rest': 'or'}]
            ti.xcom_pull(task_ids="identity", map_indexes=2),
            # 3
            ti.xcom_pull(task_ids="foo", map_indexes=0),
            # None
            ti.xcom_pull(task_ids="foo", map_indexes=2),
            # None
            ti.xcom_pull(task_ids=["identity", "plusfive"], map_indexes=2),
            # [3]
            ti.xcom_pull(task_ids=["identity", "foo", "plusfive"], map_indexes=[0, 2]),
            # [1, 3]
            ti.xcom_pull(task_ids=["identity", "plusfive", "two_parts"]),
        ]:
            print(x)

    # works
    (
        foo()
        >> identity.expand(thing=[1, 2, 3])
        >> uppercase.expand(thing=["a", "b", "c"])
        >> two_parts.expand(thing=["lorem", "ipsum", "dolor"])
        >> xcom_pull()
    )
