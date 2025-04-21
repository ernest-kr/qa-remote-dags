from datetime import datetime, timedelta
from time import sleep

from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensor, DateTimeSensorAsync
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.sdk import DAG

delays = [30, 60, 90]


@task
def get_delays():
    return delays


@task
def get_wakes(delay, **context):
    "Wake {delay} seconds after the task starts"
    ti: TaskInstance = context["ti"]
    return (ti.start_date + timedelta(seconds=delay)).isoformat()


with DAG(dag_id="datetime_mapped", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    wake_times = get_wakes.expand(delay=get_delays())

    DateTimeSensor.partial(task_id="expanded_datetime").expand(target_time=wake_times)
    TimeDeltaSensor.partial(task_id="expanded_timedelta").expand(delta=[timedelta(seconds=x) for x in [30, 60, 90]])

    DateTimeSensorAsync.partial(task_id="expanded_datetime_async").expand(target_time=wake_times)
    TimeDeltaSensorAsync.partial(task_id="expanded_timedelta_async").expand(
        delta=[timedelta(seconds=x) for x in [30, 60, 90]]
    )

    TimeDeltaSensor(task_id="static_timedelta", delta=timedelta(seconds=90))
    DateTimeSensor(
        task_id="static_datetime",
        target_time="{{macros.datetime.now() + macros.timedelta(seconds=90)}}",
    )

    PythonOperator(task_id="op_sleep_90", python_callable=lambda: sleep(90))
