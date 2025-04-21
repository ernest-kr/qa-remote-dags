"""
Example DAG to demonstrate reading dataset events annotated with extra information.

Also see examples in ``example_outlet_event_extra.py``.
"""

from __future__ import annotations

import datetime

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

ds = Dataset("s3://output/1.txt")

with DAG(
    dag_id="read_dataset_event",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["datasets"],
):

    @task(inlets=[ds])
    def read_dataset_event(*, inlet_events=None):
        for event in inlet_events[ds][:-2]:
            print(event.extra["hi"])

    read_dataset_event()

with DAG(
    dag_id="read_dataset_event_from_classic",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["dataset"],
):
    BashOperator(
        task_id="read_dataset_event_from_classic",
        inlets=[ds],
        bash_command="echo {{ inlet_events['s3://output/1.txt'][-1].extra }}",
    )
