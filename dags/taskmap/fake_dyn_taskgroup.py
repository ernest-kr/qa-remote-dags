from datetime import datetime, timedelta
from itertools import product
from textwrap import dedent

from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, chain
from airflow.utils.task_group import TaskGroup

max_n = 1000
k = 13
data = {i: i * i for i in range(max_n)}

# just some deterministic nonsense, set on repeat
story = [0, 1, 2, 0, 0, 2, 1, 2, 2, 1]
scenes = [
    {
        "foo": [230, 271, 817],
        "bar": [227, 349, 171, 835, 763],
        "baz": [798, 190, 69, 804, 382, 92],
        "qux": [12, 457, 283, 774],
        "wakka": [912, 303, 950, 942],
        "bang": [99],
        "splat": [626, 286, 139, 597],
        "tick": [303, 281, 361, 216],
        "hash": [841, 433, 12],
        "crash": [687, 252, 171, 183],
        "quark": [82, 963],
        "mark": [829, 107],
        "frob": [19, 176, 64],
        "scrozzle": [176],
        "scritch": [176],
        "ping": [230, 271, 817],
        "pong": [227, 349, 171, 835, 763],
        "sing": [798, 190, 69, 804, 382, 92],
        "song": [12, 457, 283, 774],
        "blotch": [912, 303, 950, 942],
        "skein": [99],
        "shmear": [626, 286, 139, 597],
        "blork": [303, 281, 361, 216],
        "quence": [841, 433, 12],
        "chroot": [687, 252, 171, 183],
        "chown": [82, 963],
        "moan": [829, 107],
        "yo": [19, 176, 64],
        "zot": [176],
        "orangutan": [176],
    },
    {"foo": [373], "baz": [577, 413, 471]},
    {"bar": [178, 804]},
]


@task
def pick():
    # advance to the next scene
    idx = int(Variable.get("scene", 0))
    idx = (idx + 1) % len(story)
    Variable.set("scene", idx)

    # use that scene to determine the dag structure
    structure = scenes[story[idx]]
    Variable.set("structure", structure, serialize_json=True)


@task
def wait(n):
    # nothing smart is going on here,
    # just more deterministic nonsense
    stop = datetime.now() + timedelta(seconds=n % 60 + 30)
    while datetime.now() < stop:
        for i, j in product(range(n), range(n)):
            x = data[i]
            y = data[j]
            if (x + y) % k == 0:
                print(".", end="")
            if (x - y) == 0:
                print("!")
    print("done")


with DAG(
    dag_id="fake_dyn_taskgroup_picker",
    schedule=timedelta(minutes=3),
    start_date=datetime(1970, 1, 1),
    catchup=False,
):
    pick() >> TriggerDagRunOperator(task_id="trigger_taskgroups", trigger_dag_id="fake_dyn_taskgroups")


with DAG(
    dag_id="fake_dyn_taskgroups",
    doc_md=dedent(
        """

        The taskgroup picker updates the variable every three minutes and triggers a dag.
        Sometimes, the previous dag isn't done after four minutes, so the dag gets updated
        while the previous one is still running.

        A user is doing something similar and it is going poorly for them, can we recreate
        it?
        """
    ),
    schedule=None,
    start_date=datetime(1970, 1, 1),
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    for tg, tasks in Variable.get("structure", deserialize_json=True, default_var=scenes[-1]).items():
        with TaskGroup(tg):
            inner_start = EmptyOperator(task_id="start")
            start >> inner_start
            with TaskGroup("parallel"):
                inner_start >> [wait(t) for t in tasks]
            with TaskGroup("sequential"):
                chain(inner_start, *[wait(t) for t in tasks])
