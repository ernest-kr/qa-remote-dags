from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.sdk import DAG
from pendulum import today

with DAG(
    "example_datetime_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    replace_hour = DateTimeSensor(
        task_id="60_secs_later",
        target_time="{{ ti.start_date + macros.timedelta(seconds=60) }}",
    )
