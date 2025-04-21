from datetime import datetime, timedelta

from airflow.providers.standard.sensors.time import TimeSensor
from airflow.sdk import DAG
from pendulum import today

now = datetime.now() + timedelta(minutes=3)


with DAG(
    "example_time_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    task = TimeSensor(task_id="wait_1", target_time=now.time())
