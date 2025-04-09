"""Example DAG demonstrating the usage of the XComArgs."""
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)


def generate_value():
    """Dummy function"""
    return "Bring me a shrubbery!"


@task()
def print_value(value):
    """Dummy function"""
    ctx = get_current_context()
    log.info("The knights of Ni say: %s (at %s)", value, ctx["ts"])


with DAG(
    dag_id="example_xcom_args",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule=None,
    tags=["core"],
) as dag:
    task1 = PythonOperator(
        task_id="generate_value",
        python_callable=generate_value,
    )

    print_value(task1.output)


with DAG(
    "example_xcom_args_with_operators",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule=None,
    tags=["core"],
) as dag2:
    bash_op1 = BashOperator(task_id="c", bash_command="echo c")
    bash_op2 = BashOperator(task_id="d", bash_command="echo c")
    xcom_args_a = print_value("first!")
    xcom_args_b = print_value("second!")

    bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2
