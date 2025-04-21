from datetime import datetime

from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup

foo_var = {"VAR1": "FOO"}
bar_var = {"VAR1": "BAR"}
hi_cmd = 'echo "hello $VAR1"'
bye_cmd = 'echo "goodbye $VAR1"'


@task
def envs():
    return [foo_var, bar_var]


@task
def cmds():
    return [hi_cmd, bye_cmd]


with DAG(dag_id="mapped_bash_taskgroup", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    with TaskGroup(group_id="dynamic"):
        dynamic = BashOperator.partial(task_id="bash").expand(env=envs(), bash_command=cmds())
