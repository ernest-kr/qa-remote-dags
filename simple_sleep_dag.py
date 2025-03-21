from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.utils import timezone

with DAG(
    "simple_sleep_dag",
    max_active_runs=3,
    start_date=timezone.utcnow() - timedelta(days=1)
) as dag_1:
    tasks = [
        BashOperator(
            task_id="__".join(["tasks", "{}_of_{}".format(i, "10")]),
            bash_command="echo test && sleep 30",
        )
        for i in range(1, 10 + 1)
    ]
    for i in range(1, len(tasks)):
        tasks[i].set_upstream(tasks[(i - 1) // 2])
