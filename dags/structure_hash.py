from time import sleep

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import cross_downstream
from airflow.sdk import chain
from pendulum import today


def wait_pull(**kwargs):
    ti = kwargs["ti"]
    key = ti.task_id.split(".")[-2]
    val = None
    while val is None:
        val = ti.xcom_pull(key=key)
        if not val:
            print("not yet")
        sleep(5)
    print(f"got {val}")
    return val


def push(val, **kwargs):
    ti = kwargs["ti"]
    key = ti.task_id.split(".")[-2]
    ti.xcom_push(key=key, value=val)


@task
def x(**kwargs):
    push("X", **kwargs)
    return "x"


@task
def y(a, **kwargs):
    b = wait_pull(**kwargs)
    push(f"{b}Y", **kwargs)
    return f"{a}y"


@task
def z(a, **kwargs):
    b = wait_pull(**kwargs)
    push(f"{b}Z", **kwargs)
    return f"{a}z"


@task
def get(**kwargs):
    return wait_pull(**kwargs)


@task
def cat(a, b):
    if not a:
        return b
    if not b:
        return a
    return a + b


@task
def assert_one_of(to):
    @task
    def approved(val):
        print(val)
        assert int(val) in to

    return approved


@task_group()
def separate():
    a = x()
    y(a)
    c = z(a)
    d = get()
    return cat(c, d)


@task_group()
def chained_one_way():
    a = x()
    b = y(a)
    c = z(a)
    d = get()
    chain(a, b, c, d)
    return cat(c, d)


@task_group()
def chained_another_way():
    a = x()
    b = y(a)
    c = z(a)
    d = get()
    b >> d  # to remove race condition
    chain(a, c, b, d)
    return cat(c, d)


@task_group()
def crossed_one_way():
    a = x()
    b = y(a)
    c = z(a)
    d = get()
    c >> d  # to remove race condition
    cross_downstream([a, b], [c, d])
    return cat(c, d)


@task_group()
def crossed_another_way():
    a = x()
    b = y(a)
    c = z(a)
    d = get()
    cross_downstream([a, c], [b, d])
    return cat(c, d)


@task
def assert_equal(a, b):
    print(f"{a} == {b}")
    assert a == b


@dag(
    schedule=None,
    start_date=today("UTC").add(days=-1),
    default_args={"owner": "airflow"},
    max_active_runs=1,
    catchup=False,
    tags=["core"],
)
def cross_chain_structure_hash():
    """
    The patterns below aren't supposed to be meaningful, it's enough that a
    change in the underlying functionality will fail this test and attract
    human attention.
    """

    assert_equal("xzX", separate())
    assert_equal("xzXYZ", chained_one_way())
    assert_equal("xzXZY", chained_another_way())
    assert_equal("xzXYZ", crossed_one_way())
    assert_equal("xzXZ", crossed_another_way())


the_dag = cross_chain_structure_hash()
