from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
This dag tests the dag parameter 'user_defined_macros' which allows the user to create their own jinja template macros
##Expected Behavior
This dag has 2 tasks that are both expected to succeed.\n
If either one or both of the tasks fail there is a problem with the 'user_defined_macros' parameter.
"""

with DAG(
    dag_id="user_defined_macros_params",
    start_date=datetime(2020, 12, 10),
    schedule=None,
    user_defined_macros={"cryptic": "$h3jdg^^@d"},
    doc_md=docs,
    tags=["dagparams"],
) as dag:
    B0 = BashOperator(
        task_id="bash_op",
        bash_command="echo '{{ cryptic }}'",
    )

    py0 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        op_args=["bash_op", "$h3jdg^^@d", "%%87w4e3s"],
    )


B0 >> py0
