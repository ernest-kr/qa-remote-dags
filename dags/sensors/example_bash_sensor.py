from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG
from pendulum import today

date = "{{ ds }}"

with DAG(
    "example_bash_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    task1 = BashSensor(task_id="sleep_10", bash_command="sleep 10")
    task2 = BashSensor(
        task_id="sleep_total",
        bash_command="echo $EXECUTION_DATE",
        env={"EXECUTION_DATE": date},
    )
