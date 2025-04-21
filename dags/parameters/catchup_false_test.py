from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

start_dated = datetime(2018, 11, 28)


def ketchup(**context):
    the_dag = context["dag"]
    exec_date = str(the_dag.start_date)
    start_dated = datetime(2018, 11, 28)
    exec_date = exec_date[0:10]
    start_dated = str(start_dated)[0:10]
    print(f"This is the start date: {start_dated} and this is the execution date: {exec_date}")
    assert start_dated == exec_date


with DAG(
    dag_id="catchup_false_test",
    start_date=start_dated,
    schedule=timedelta(minutes=3),
    catchup=False,
    tags=["dagparams"],
) as dag:
    t1 = PythonOperator(task_id="catchup", python_callable=ketchup)
