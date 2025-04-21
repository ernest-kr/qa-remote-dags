from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from airflow.utils.trigger_rule import TriggerRule
from pendulum import today
from plugins.airflow_dag_introspection import assert_the_task_states

left = Asset(name="asset_name")
right = Asset(name="asset_name", uri="asset_uri")

with DAG(
    dag_id="asset_with_duplicate_name",
    start_date=today("UTC").add(days=-2),
    schedule=None,
    max_active_runs=1,
    tags=["asset", "AIP-74"],
) as dag:
    task1 = EmptyOperator(task_id="task1", outlets=[left, right])
    status = PythonOperator(
        task_id="assert_task_status",
        python_callable=assert_the_task_states,
        op_args=[{"task1": "failed"}],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    task1 >> status
