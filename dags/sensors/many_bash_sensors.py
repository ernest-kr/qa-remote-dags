from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG


def get_waiter(a, i):
    with DAG(
        f"sensors_{a}",
        start_date=datetime(1970, 1, 1),
        # schedule=timedelta(seconds=i),
        schedule=None,
        # max_active_runs=1,
        catchup=False,
        tags=["sensor"],
    ) as dag:
        [
            BashSensor(
                task_id=f"sleep_{j}",
                bash_command=f"sleep {j} && python -c 'import random, sys; sys.exit(random.randint(0,1))'",
            )
            >> BashOperator(task_id=f"done_{j}", bash_command="echo done")
            for j in range(5)
        ] >> EmptyOperator(task_id="done_all")
    return dag


# just some primes for maximal chaos
a = get_waiter("a", 41)
b = get_waiter("b", 53)
c = get_waiter("c", 67)
d = get_waiter("d", 73)
e = get_waiter("e", 97)
