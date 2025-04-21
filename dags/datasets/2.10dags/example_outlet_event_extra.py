"""
Example DAG to demonstrate annotating a dataset event with extra information.

Also see examples in ``example_inlet_event_extra.py``.
"""

from __future__ import annotations

import datetime

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Metadata

ds = Dataset("s3://output/1.txt")

with DAG(
    dag_id="dataset_with_extra_by_yield",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["datasets"],
):

    @task(outlets=[ds])
    def dataset_with_extra_by_yield():
        yield Metadata(ds, {"hi": "bye"})

    dataset_with_extra_by_yield()

with DAG(
    dag_id="dataset_with_extra_by_context",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["dataset"],
):

    @task(outlets=[ds])
    def dataset_with_extra_by_context(*, outlet_events=None):
        outlet_events[ds].extra = {"hi": "bye"}

    dataset_with_extra_by_context()

with DAG(
    dag_id="dataset_with_extra_from_classic_operator",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule="@daily",
    tags=["dataset"],
):

    def _dataset_with_extra_from_classic_operator_post_execute(context, result):
        context["outlet_events"][ds].extra = {"hi": "bye"}

    BashOperator(
        task_id="dataset_with_extra_from_classic_operator",
        outlets=[ds],
        bash_command=":",
        post_execute=_dataset_with_extra_from_classic_operator_post_execute,
    )
