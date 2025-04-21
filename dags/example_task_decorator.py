from airflow.decorators import task
from airflow.sdk import DAG
from pendulum import today


@task
def people():
    return [{"name": "Mark", "age": 10}, {"name": "Jack", "age": 11}]


@task
def list_people(people):
    for item in people:
        print(item["name"], "is", item["age"], "years old")


with DAG(
    "example_at_task_decorator",
    default_args={"owner": "airflow"},
    start_date=today("UTC").add(days=-2),
    schedule=None,
    tags=["core"],
) as dag:
    task1 = people()
    list_people(task1)
