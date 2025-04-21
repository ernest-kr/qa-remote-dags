from airflow.decorators import dag, task
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

@task
def my_task(task_id):
    print(f"Task {task_id} executed")
    return f"Task {task_id} done"

@dag(
    schedule_interval="*/5 * * * *",  # Run every 5 minutes (adjust as needed)
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
    catchup=False,
)
def massive_async_dag():
    """DAG with 2000 tasks and an async sensor."""

    # Create 2000 tasks
    tasks = [my_task(task_id=f"task_{i}") for i in range(2000)]

    # Add an async sensor in the middle
    async_sensor = DateTimeSensorAsync(
        task_id="wait_3_min",
        target_time="{{ execution_date.add(minutes=3) }}",
    )

    # Define the workflow: tasks -> sensor -> tasks
    first_half = tasks[:1000]
    second_half = tasks[1000:]

    for t in first_half:
      t

    async_sensor

    for t in second_half:
      t

massive_dag = massive_async_dag()