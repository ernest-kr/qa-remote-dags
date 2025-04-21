from datetime import datetime
from textwrap import dedent

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="jinja_mapped",
    schedule=None,
    start_date=datetime(1970, 1, 1),
    tags=["is_standalone_test", "turbulence_suite_aip42", "taskmap"],
    doc_md=dedent(
        """
        This dag tests whether unmapped parameters can reference data stored by mapped parameters.
        The ability to do so was added in 2.4.1 in PR https://github.com/apache/airflow/pull/26702
        Expect two mapped instances for both tasks: one says "hello" and the other than says "goodbye"

        The normal usage of `env` is to set environment variables.  This *also* sets an environment
        variable, but the commmand ignors it.  Instead, we're just using "env" to store the greeting
        and we're building the bash_command to include it.
        """
    ),
) as dag:
    BashOperator.partial(
        task_id="expand",
        bash_command="echo {{ task.env['greeting'] }}",
    ).expand(
        env=[
            {"greeting": "hello"},
            {"greeting": "goodbye"},
        ]
    )

    BashOperator.partial(
        task_id="expand_kwargs",
        bash_command="echo {{ task.env['greeting'] }}",
    ).expand_kwargs(
        [
            {"env": {"greeting": "hello"}},
            {"env": {"greeting": "goodbye"}},
        ]
    )
