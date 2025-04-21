from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
This dag tests that the dag parameter 'params' works correctly.
The 1st task runs a bash templated command using the params set in the dag_params.\n
The 2nd task checks that the value is the same as defined in 'params'.\n
####Expected Behavior
Both tasks are expected to succeed. If either one or both tasks fail something is wrong with the 'params' parameter.
"""

with DAG(
    dag_id="params_test",
    start_date=today("UTC").add(days=-1),
    schedule=None,
    params={"cryptic": "$eh5f6^^"},
    doc_md=docs,
    tags=["dagparams"],
) as dag:
    templated_command = """
        {% for i in range(3) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.cryptic }}"
        {% endfor %}
    """

    B0 = BashOperator(
        task_id="templated",
        bash_command=templated_command,
    )

    py0 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        op_args=["templated", "$eh5f6^^", "&**dh63"],
    )

B0 >> py0
