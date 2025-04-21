from datetime import datetime, timedelta

from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.sdk import DAG

with DAG(dag_id="timedelta_both", start_date=datetime(1970, 1, 1), schedule=None, tags=["sensor"]) as dag:
    TimeDeltaSensor(task_id="sync_30", delta=timedelta(seconds=90), poke_interval=30)
    TimeDeltaSensorAsync(task_id="async_30", delta=timedelta(seconds=90), poke_interval=30)
    TimeDeltaSensor(task_id="sync", delta=timedelta(seconds=90))
    TimeDeltaSensorAsync(task_id="async", delta=timedelta(seconds=90))
