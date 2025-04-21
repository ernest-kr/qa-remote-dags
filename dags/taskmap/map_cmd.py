from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(dag_id="map_cmd", schedule=None, start_date=datetime(2001, 1, 1), tags=["taskmap"]) as dag:

    @dag.task
    def orig_data():
        return [
            {"cmd": "echo $VAR", "env": {"VAR": "hello"}},
            {"cmd": "echo $VAR $VAR", "env": {"VAR": "goodbye"}},
        ]

    def get_cmd(x):
        return x["cmd"]

    def get_env(x):
        return x["env"]

    BashOperator.partial(
        task_id="four_cmds",
    ).expand(env=orig_data().map(get_env), bash_command=orig_data().map(get_cmd))
