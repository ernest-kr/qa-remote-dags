from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
This Dag tests that when you try to use a nonexistent macro a jinja error is returned in airflow's logs
####Expected Behavior
This dag has 2 tasks.\n
The first task is expected to fail because that task is using a macro that doesn't exist.\n
The second task is expected to succeed by checking the previous task's logs for a jinja error.
"""


def user_macro1(num: int) -> str:
    squares = []
    for i in range(1, num):
        if i * i == num:
            squares.append(i)
            return f"{num} is a square number {i} and {i} are its roots"
    # return f"The squares between 1 and {num} are: {squares}"


def user_macro2(num):
    primes = []
    for i in range(2, num):
        if i % 2 != 0 and i % 3 != 0 and i % 5 != 0 and i % 7 != 0:
            primes.append(i)
    return f"The prime numbers between 1 and {num} are {primes}"


def check_macros(func1, func2):
    return func1, func2


with DAG(
    dag_id="non_existent_macros",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    user_defined_macros={"macro1": user_macro1(225), "macro2": user_macro2(100)},
    doc_md=docs,
    tags=["core"],
) as dag:
    py0 = PythonOperator(
        task_id="check_user_defined_macros",
        python_callable=check_macros,
        op_args=["{{ macro1 }}", "{{ macro3 }}"],
    )

    py1 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        op_args=[
            "check_user_defined_macros",
            "'macro3' is undefined",
            "Returned value was: ('225 is a square number 15 and 15 are its roots', 'The prime numbers between 1 and 100 are [11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]')",
        ],
        trigger_rule="one_failed",
    )

py0 >> py1
