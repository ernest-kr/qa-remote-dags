from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_async_triggerer_dag",
    start_date=datetime(2026, 2, 1),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    tags=["async", "triggerer", "backfill"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    wait_async = TimeDeltaSensorAsync(
        task_id="wait_30_seconds_async",
        delta=timedelta(seconds=30),
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> wait_async >> end
