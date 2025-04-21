from airflow.decorators import dag, task
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from pendulum import today


@task
def before():
    print("before")


@task
def after():
    print("after")


@dag(
    schedule="*/2 * * * *",
    start_date=today("UTC").add(days=-1),
    default_args={"owner": "airflow"},
    catchup=False,
)
def each_two_wait_one_async():
    """
    Execute after each two-minute interval, and
    finish no earlier than one minutes after that interval starts.
    """

    (
        before()
        >> DateTimeSensorAsync(
            task_id="wait_exec_plus_one",
            target_time="{{ logical_date.add(minutes=1) }}",
        )
        >> after()
    )


the_async_dag = each_two_wait_one_async()
