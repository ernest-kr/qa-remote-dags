from datetime import datetime
from itertools import product

from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

foo_var = {"VAR1": "FOO"}
bar_var = {"VAR1": "BAR"}
hi_cmd = 'echo "hello $VAR1"'
bye_cmd = 'echo "goodbye $VAR1"'


@task
def envs():
    return [foo_var, bar_var]


@task
def cmds():
    return [hi_cmd, bye_cmd]


@task
def xcom_check(mapped_id, static_ids, **context):
    ti: TaskInstance = context["ti"]

    mapped = []
    for i in range(4):
        mapped.append(str(ti.xcom_pull(task_ids=mapped_id, map_indexes=i)))

    static = []
    for _id in static_ids:
        static.append(str(ti.xcom_pull(task_ids=_id)))

    mapped = sorted(mapped)
    static = sorted(static)

    for m, s in zip(mapped, static):
        print("mapped_xcom:", m, "static_xcom:", s)
    for m, s in zip(mapped, static):
        assert m == s
        assert m != "None"
        assert m is not None


with DAG(
    dag_id="mapped_bash_xcom",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["taskmap"],
    doc_md="ensure that mapped and static commands produce identical output",
) as dag:
    dynamic = BashOperator.partial(task_id="bash", do_xcom_push=True).expand(env=envs(), bash_command=cmds())

    s = EmptyOperator(task_id="static_start")
    d = EmptyOperator(task_id="static_done")

    static_ids = []
    for i, (e, c) in enumerate(product([foo_var, bar_var], [hi_cmd, bye_cmd])):
        task = BashOperator(task_id=f"bash{i + 1}", env=e, bash_command=c, do_xcom_push=True)
        (s >> task >> d)
        static_ids.append(task.task_id)

    [d, dynamic] >> xcom_check(dynamic.task_id, static_ids)
