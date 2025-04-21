from datetime import datetime

from airflow.decorators import task
from airflow.sdk import DAG


@task
def make_list():
    return [[1, 2], [2, 3], [{"a": "b"}, "foo"]]


@task
def consumer(val1=None, val2=None):
    print(val1)
    print(val2)


with DAG(dag_id="test_mapped_taskflow", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    consumer.expand(val1=make_list())
