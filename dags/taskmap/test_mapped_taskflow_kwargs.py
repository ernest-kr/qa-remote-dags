from datetime import datetime

from airflow.decorators import task
from airflow.sdk import DAG


@task
def make_list():
    return [{"foo": "baz", "bar": "qux"}, {"foo": "wakka", "bar": "bang"}]


@task
def getfoos(args):
    return [x["foo"] for x in args]


@task
def getbars(args):
    return [x["bar"] for x in args]


@task
def consumer(foo, bar):
    print("foo", foo)
    print("bar", bar)


with DAG(dag_id="fromdict_apply_taskflow", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    args = make_list()
    consumer.expand(foo=getfoos(args=args), bar=getbars(args=args))
