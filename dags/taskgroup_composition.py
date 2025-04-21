from datetime import datetime

from airflow.decorators import task, task_group
from airflow.sdk import DAG


@task
def incr(x):
    return x + 1


@task
def add(x, y):
    return x + y


@task_group
def increment_each_twice_then_add(x, y):
    return add(incr(incr(x)), incr(incr(y)))


@task
def print_it(it):
    print(it)


with DAG(dag_id="taskgroup_composition", start_date=datetime(1970, 1, 1), schedule=None, tags=["core"]) as dag:
    one = increment_each_twice_then_add(1, 2)
    other = increment_each_twice_then_add(3, 4)
    both = increment_each_twice_then_add(one, other)
    print_it(incr(both))
