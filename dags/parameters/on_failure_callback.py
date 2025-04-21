from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from plugins.airflow_dag_introspection import log_checker

docs = """
####Note:
for 'on_execute_callback', 'on_success_callback, and 'on_failure_callback' this dag parameter needs to be called at the task level it seems.
####Purpose
This dag tests that dag parameter 'on_failure_callback' calls the callback function defined called 'mycallback'.
####Expected_Behavior
This dag has 2 tasks. The first task is expected to fail and the second task is expected to succeed.\n
The first task is a PythonOperator that has the 'on_failure_callback' parameter which prints a statement to that tasks logs.\n
The second task is a PythonOperator that checks the logs of the previous task to ensure the print statement appeared from the callback function.
"""


default_args = {
    "owner": "airflow",
    "start_date": datetime(2018, 10, 31),
}


def fail():
    raise Exception


def mycallback(context):
    print("I HAVE BEEN CALLED! " * 4)


with DAG(
    dag_id="on_failure_callback",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    doc_md=docs,
    tags=["core"],
) as dag:
    test1 = PythonOperator(
        task_id="print_to_logs",
        python_callable=fail,
        on_failure_callback=mycallback,
    )

    test2 = PythonOperator(
        task_id="check_logs",
        retries=5,
        trigger_rule="one_failed",
        python_callable=log_checker,
        op_args=["print_to_logs", "I HAVE BEEN CALLED!", "I HAVEN'T BEEN CALLED!"],
    )

test1 >> test2
