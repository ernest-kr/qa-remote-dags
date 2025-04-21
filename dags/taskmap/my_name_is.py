from datetime import datetime

from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.sdk import DAG


@task
def mynameis(arg, **context):
    ti: TaskInstance = context["ti"]
    print(ti.task_id)
    print(arg)


with DAG(dag_id="my_name_is", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    mynameis.expand(arg=["slim", "shady"])
