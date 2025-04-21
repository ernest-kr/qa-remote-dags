from datetime import datetime

from airflow.decorators import task
from airflow.sdk import DAG

# source: https://astronomer.slack.com/archives/CGQSYG25V/p1668705900788699?thread_ts=1668643084.163769&cid=CGQSYG25V

with DAG(dag_id="many_empty", start_date=datetime(1970, 1, 1), catchup=False, tags=["taskmap"]) as dag:

    @task
    def add_one(x: int):
        return x + 1

    @task
    def say_hi():
        print("Hi")

    @task
    def say_bye():
        print("Bye")

    added_values = add_one.expand(x=[])
    added_more_values = add_one.expand(x=[])
    added_more_more_values = add_one.expand(x=[])
    added_values >> added_more_values >> added_more_more_values
