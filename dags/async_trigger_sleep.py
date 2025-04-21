from datetime import datetime

from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from airflow.sdk import DAG
from pendulum import today

with DAG("async_trigger_sleep", schedule=None, start_date=today("UTC").add(days=-1), tags=["async_migration"]) as dag:
    DateTimeSensorAsync(
        task_id="wait_for_start",
        target_time=datetime(2028, 12, 10),
    )
