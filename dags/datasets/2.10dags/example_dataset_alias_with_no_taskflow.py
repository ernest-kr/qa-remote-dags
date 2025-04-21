"""
Example DAG for demonstrating the behavior of the DatasetAlias feature in Airflow, including conditional and
dataset expression-based scheduling.
Notes on usage:
Turn on all the DAGs.
Before running any DAG, the schedule of the "dataset_alias_example_alias_consumer_with_no_taskflow" DAG will show as "unresolved DatasetAlias".
This is expected because the dataset alias has not been resolved into any dataset yet.
Once the "dataset_s3_bucket_producer_with_no_taskflow" DAG is triggered, the "dataset_s3_bucket_consumer_with_no_taskflow" DAG should be triggered upon completion.
This is because the dataset alias "example-alias-no-taskflow" is used to add a dataset event to the dataset "s3://bucket/my-task-with-no-taskflow"
during the "produce_dataset_events_through_dataset_alias_with_no_taskflow" task. Also, the schedule of the "dataset_alias_example_alias_consumer_with_no_taskflow" DAG should change to "Dataset" as
the dataset alias "example-alias-no-taskflow" is now resolved to the dataset "s3://bucket/my-task-with-no-taskflow" and this DAG should also be triggered.
"""

from __future__ import annotations

import pendulum
from airflow.datasets import Dataset, DatasetAlias
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    dag_id="dataset_s3_bucket_producer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["datasets"],
):

    def produce_dataset_events():
        pass

    PythonOperator(
        task_id="produce_dataset_events",
        outlets=[Dataset("s3://bucket/my-task-with-no-taskflow")],
        python_callable=produce_dataset_events,
    )


with DAG(
    dag_id="dataset_alias_example_alias_producer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dataset"],
):

    def produce_dataset_events_through_dataset_alias_with_no_taskflow(*, outlet_events=None):
        bucket_name = "bucket"
        object_path = "my-task"
        outlet_events[DatasetAlias("example-alias-no-taskflow")].add(Dataset(f"s3://{bucket_name}/{object_path}"))

    PythonOperator(
        task_id="produce_dataset_events_through_dataset_alias_with_no_taskflow",
        outlets=[DatasetAlias("example-alias-no-taskflow")],
        python_callable=produce_dataset_events_through_dataset_alias_with_no_taskflow,
    )

with DAG(
    dag_id="dataset_s3_bucket_consumer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[Dataset("s3://bucket/my-task-with-no-taskflow")],
    catchup=False,
    tags=["dataset"],
):

    def consume_dataset_event():
        pass

    PythonOperator(task_id="consume_dataset_event", python_callable=consume_dataset_event)

with DAG(
    dag_id="dataset_alias_example_alias_consumer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[DatasetAlias("example-alias-no-taskflow")],
    catchup=False,
    tags=["dataset"],
):

    def consume_dataset_event_from_dataset_alias(*, inlet_events=None):
        for event in inlet_events[DatasetAlias("example-alias-no-taskflow")]:
            print(event)

    PythonOperator(
        task_id="consume_dataset_event_from_dataset_alias",
        python_callable=consume_dataset_event_from_dataset_alias,
        inlets=[DatasetAlias("example-alias-no-taskflow")],
    )
