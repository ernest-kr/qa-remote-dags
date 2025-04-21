from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="setup_teardown_parallel",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["setup_teardown"],
) as dag:
    root_setup = BashOperator(task_id="root_setup", bash_command="sleep 5")
    root_setup1 = BashOperator(task_id="root_setup1", bash_command="sleep 5")
    root_normal = BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
    root_teardown = BashOperator(task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'")
    root_teardown1 = BashOperator(task_id="root_teardown1", bash_command="echo 'Goodbye from root_teardown'")

    (
        [root_setup, root_setup1]
        >> root_normal
        >> [
            root_teardown.as_teardown(setups=[root_setup, root_setup1]),
            root_teardown1.as_teardown(setups=[root_setup, root_setup1]),
        ]
    )
