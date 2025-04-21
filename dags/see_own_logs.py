from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import today
from plugins.api_utility import get_task_instance_log, get_task_instances


def get_logs(try_number=1, **context):
    # get the task instances for the other tasks in this dag
    dag_id = context["dag_run"].dag_id
    run_id = context["dag_run"].run_id
    tis_response = get_task_instances(dag_id, run_id)
    task_instances = tis_response.json()["task_instances"]
    hello_task_instance = next(filter(lambda ti: ti.get("task_id") == "hello", task_instances))
    world_task_instance = next(filter(lambda ti: ti.get("task_id") == "world", task_instances))

    # read their logs
    def check(ti, expect, notexpect):
        task_id = ti.get("task_id")
        log_response = get_task_instance_log(dag_id, run_id, task_id, try_number)
        print(f"Found logs: '''{log_response.text}'''")
        assert expect in log_response.text
        assert notexpect not in log_response.text
        print(f"Found '''{expect}''' but not '''{notexpect}'''")

    # make sure expected output appeared
    check(hello_task_instance, "Done. Returned value was: Hello", "World")
    check(world_task_instance, "Done. Returned value was: World", "Hello")


@dag(
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["core"],
)
def see_own_logs():
    """
    The last task will fail if logs are mishandled.
    For instance: https://github.com/apache/airflow/issues/19058
    """
    (
        PythonOperator(task_id="hello", python_callable=lambda: "Hello")
        >> PythonOperator(task_id="world", python_callable=lambda: "World")
        >> PythonOperator(task_id="get_logs", python_callable=get_logs)
    )


the_dag = see_own_logs()
