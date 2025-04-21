from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow_dag_introspection import log_checker
from pendulum import today

docs = """
####Note:
for 'on_execute_callback', 'on_success_callback, and 'on_failure_callback' this dag parameter needs to be called at the task level it seems.
####Purpose
This dag tests that dag parameter 'on_execute_callback' calls the callback function defined called 'mycallback'.
####Expected_Behavior
This dag has 2 tasks both of which are expected to succeed.\n
The first task is a PythonOperator that has the 'on_execute_callback' parameter which prints a statement to that tasks logs.\n
The second task is a PythonOperator that checks the logs of the previous task to ensure the print statement appeared from the callback function.
"""


def passed():
    pass


def mycallback(context):
    print("I HAVE BEEN CALLED! " * 4)


with DAG(
    dag_id="on_execute_callback",
    start_date=today("UTC").add(days=-1),
    schedule=None,
    catchup=False,
    doc_md=docs,
    tags=["core"],
) as dag:
    test1 = PythonOperator(task_id="print_to_logs", python_callable=passed, on_execute_callback=mycallback)

    test2 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        op_args=["print_to_logs", "I HAVE BEEN CALLED!", "I HAVENT BEEN CALLED"],
    )

test1 >> test2
