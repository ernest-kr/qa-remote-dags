import datetime

from airflow.decorators import task
from airflow.sdk import DAG, Asset, Metadata

inlet = Asset(name="asset_inlet")
outlet = Asset(name="asset_outlet")

with DAG(
    dag_id="test_asset_event_producer",
    start_date=datetime.datetime.min,
    schedule=None,
    tags=["asset", "AIP-74"],
    is_paused_upon_creation=False,
) as dag:

    @task(outlets=[outlet])
    def asset_with_extra_by_yield():
        yield Metadata(outlet, {"hi": "bye"})

    asset_with_extra_by_yield()

with DAG(
    dag_id="test_asset_event_consumer",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule=None,
    tags=["asset", "AIP-74"],
):

    @task(inlets=[inlet])
    def read_dataset_event(*, inlet_events=None):
        for event in inlet_events[inlet][:-2]:
            print(event.extra["hi"])

    read_dataset_event()
