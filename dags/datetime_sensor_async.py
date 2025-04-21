from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime
from plugins.airflow_dag_introspection import assert_the_task_states

with DAG(
    dag_id="example_deferrable_dag",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["async"],
) as dag:
    start = EmptyOperator(task_id="start")

    wait = DateTimeSensorAsync(
        task_id="wait_for_time", target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=1) }}"""
    )

    end = EmptyOperator(task_id="end")

    status = PythonOperator(
        task_id="assert_task_status",
        python_callable=assert_the_task_states,
        op_args=[{"wait_for_time": "success"}],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> wait >> end >> status
