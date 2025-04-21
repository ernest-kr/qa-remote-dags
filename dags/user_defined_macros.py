from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import log_checker

docs = """
####Purpose
This dag tests that the user_defined_macros which allow the user to define their own custom macros works properly.
####Expected Behavior
This dag has 2 tasks that are both expected to succeed. If one or both tasks fail then there is a problem with 'user_defined_macros'.\n
The first task returns the 'macro1' and 'macro2' functions.\n
The second task checks the logs of the first task to ensure the custom macro functions printed to the logs
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
    dag_id="user_defined_macros",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    user_defined_macros={"macro1": user_macro1(225), "macro2": user_macro2(100)},
    doc_md=docs,
    tags=["core"],
) as dag:
    py0 = PythonOperator(
        task_id="check_user_defined_macros",
        python_callable=check_macros,
        op_args=["{{ macro1 }}", "{{ macro2 }}"],
    )

    py1 = PythonOperator(
        task_id="check_the_logs",
        python_callable=log_checker,
        op_args=[
            "check_user_defined_macros",
            "Returned value was: ('225 is a square number 15 and 15 are its roots', 'The prime numbers between 1 and 100 are [11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]')",
            "Done. Returned value was: None",
        ],
    )

py0 >> py1
