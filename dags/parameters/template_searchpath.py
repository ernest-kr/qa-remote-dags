from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow_dag_introspection import log_checker
from pendulum import today

docs = """
####Context
'template_searchpath' allows you to define a filepath for templated files.
####Purpose
This dag tests that the dag parameter 'template_searchpath' works correctly.
####Expected Behavior
This dag has 2 tasks both of which should succeed. If either one or both tasks fail there is a problem with the dag parameter 'template_searchpath'.\n
The 1st task runs a bash command that uses a templated file stored in the path defined in 'template_searchpath' that prints out a cryptic looking message.\n
The 2nd task checks the logs of the 1st task to ensure the cryptic looking templated string was ran by the 'BashOperator' in the 1st task.
"""


with DAG(
    dag_id="template_searchpath",
    schedule=None,
    start_date=today("UTC").add(days=-1),
    template_searchpath=["/usr/local/airflow/include/"],
    user_defined_macros={"cryptic": "F2sx4Ujm"},
    doc_md=docs,
    tags=["dagparams"],
) as dag:
    t1 = BashOperator(task_id="run_templated_command", bash_command="template_searchpath.sh")

    t2 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        # op_args=["run_templated_command", "{{ cryptic }}", "Td2l9M"],
        # let's try asserting the actual value instead of the templated value.
        op_args=["run_templated_command", "{{ cryptic }}", "Td2l9M"],
    )
