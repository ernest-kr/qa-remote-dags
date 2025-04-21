import datetime

from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from airflow.utils.trigger_rule import TriggerRule
from plugins.airflow_dag_introspection import assert_the_task_states

asset_controller = Asset(name="asset_controller")

with DAG(
    dag_id="test_asset_scheduled_controller",
    start_date=datetime.datetime.min,
    schedule=None,
    tags=["asset", "AIP-74"],
) as dag:

    @task(outlets=[asset_controller])
    def produce_dataset_events():
        pass

    produce_dataset_events()

with DAG(
    dag_id="test_asset_scheduled_trigger",
    catchup=False,
    start_date=datetime.datetime.min,
    schedule=[asset_controller],
    tags=["asset", "AIP-74"],
):

    @task()
    def consume_dataset_event():
        pass

    status = PythonOperator(
        task_id="assert_task_status",
        python_callable=assert_the_task_states,
        op_args=[{"consume_dataset_event": "success"}],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    consume_dataset_event() >> status
