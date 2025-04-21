from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

old_import_asset = Asset("old_import_asset")
two_days = datetime.now() - timedelta(days=2)

dag = DAG("test_old_import_dag", schedule="@daily", catchup=False, tags=["core"])

hello_task = BashOperator(
    task_id="test_task",
    bash_command='echo "Hello World from Airflow!"',
    do_xcom_push=True,
    dag=dag,
)

hello_task


with DAG(dag_id="test_old_import_asset", schedule="@daily", catchup=False, tags=["core"]) as dag:
    task = EmptyOperator(task_id="test_task", outlets=[old_import_asset])

    task

with DAG(
    dag_id="test_old_import_chain",
    schedule=None,
    start_date=two_days,
    tags=["core"],
) as dag:
    with TaskGroup(group_id="group1") as taskgroup1:
        t1 = EmptyOperator(task_id="dummmy1")
        t2 = EmptyOperator(task_id="dummy2")
        t3 = EmptyOperator(task_id="dummy3")

    t4 = EmptyOperator(task_id="dummy4")

chain(
    [Label("branching to group tasks"), Label("stuff")],
    taskgroup1,
    t4,
)
