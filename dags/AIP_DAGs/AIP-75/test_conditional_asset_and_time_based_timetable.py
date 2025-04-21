from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset
from airflow.sdk.definitions.asset.decorators import asset
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@asset(schedule=None, tags=["asset", "AIP-75"])
def conditional_asset1():
    pass


@asset(schedule=None, tags=["asset", "AIP-75"])
def conditional_asset2():
    pass


with DAG(
    dag_id="conditional_asset_and_time_based_timetable",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/3 * * * *", timezone="UTC"), assets=(conditional_asset1 | conditional_asset2)
    ),
    tags=["AIP-75", "asset"],
) as dag:
    BashOperator(
        outlets=[Asset("s3://asset_time_based/asset_other_unknown.txt")],
        task_id="conditional_asset_and_time_based_timetable",
        bash_command="sleep 5",
    )
