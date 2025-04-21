from datetime import datetime
from random import randint
from textwrap import dedent

from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator


@task
def flip_coin():
    if randint(0, 1):
        return "Heads"
    else:
        return "Tails"


def decide(coin_result):
    print(coin_result)
    if "Heads" in coin_result:
        print("dont_skip")
        return True
    else:
        print("skip")
        return False


def do_thing(msg):
    "Creates a task with {msg} for a task_id that prints {msg}"

    @task(task_id=msg)
    def f(msg):
        print(msg)

    return f(msg)


@dag(
    start_date=datetime(year=1970, month=1, day=1),
    schedule=None,
    tags=["short_circuit", "core"],
    doc_md=dedent(
        """
        If the ShortCircuitOperator's callable returns False, it should skip all
        downstream task until the next join (which in this DAG I've named "join")

        This DAG flips a coin and uses that value to control the ShortCircuitOperator.

        Expectations:

        - no tasks fail
        - these tasks succeed:
            - run_always
            - also run always
        - this task sometimes succeeds and is sometimes skip:
            - skip_on_tails

        When I run it via `astro dev start` I find that something goes wrong and the
        return value of 'decide' never shows up in XCOM.  Also, tasks never get skipped,
        even when the coin lands on "Tails", they all succeed.

        Am I missing something obvious, or am I looking at a bug?
        """
    ),
)
def short_circuit_sometimes_skip():
    decision = flip_coin()
    done = EmptyOperator(task_id="join")
    (
        decision
        >> ShortCircuitOperator(task_id="decide", op_args=[decision], python_callable=decide)
        >> do_thing("skip_on_tails")
        >> done
    )
    decision >> do_thing("run_always") >> done >> do_thing("also_run_always")


the_dag = short_circuit_sometimes_skip()
