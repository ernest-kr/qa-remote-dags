from airflow.providers.standard.sensors.weekday import DayOfWeekSensor
from airflow.sdk import DAG
from pendulum import today

with DAG(
    "example_week_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    working_day_check = DayOfWeekSensor(
        task_id="working_day_check",
        # added weekend days so that no matter what any day of the week this sensor will work.
        week_day={"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"},
        use_task_logical_date=True,
        poke_interval=20.0,
        timeout=120.0,
        dag=dag,
    )
