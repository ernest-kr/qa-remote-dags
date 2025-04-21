from datetime import timedelta

from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG
from pendulum import today

with DAG(
    "example_timedelta_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    task = TimeDeltaSensor(task_id="wait_1", delta=-timedelta(days=1))
