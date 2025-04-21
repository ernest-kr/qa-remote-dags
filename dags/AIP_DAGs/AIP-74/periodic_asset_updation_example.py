import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from airflow.utils.trigger_rule import TriggerRule
from plugins.airflow_dag_introspection import assert_the_task_states

periodic_asset = Asset(name="periodic_asset", uri="s3://periodic_asset/output_1.txt")

with DAG(
    dag_id="periodic_asset_update",
    catchup=True,
    start_date=pendulum.today("UTC").add(days=-5),
    schedule="@daily",
    tags=["asset", "AIP-74"],
) as dag:
    periodic_asset = EmptyOperator(task_id="periodic_asset", outlets=[periodic_asset])
    status = PythonOperator(
        task_id="assert_task_status",
        python_callable=assert_the_task_states,
        op_args=[{"periodic_asset": "success"}],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    periodic_asset >> status
