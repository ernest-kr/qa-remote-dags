from datetime import datetime

from airflow import Dataset
from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

# these two will have downstream dags
left = Dataset("left")
right = Dataset("right")


@dag(start_date=datetime(1970, 1, 1), schedule=None)
def branch_empty():
    l = EmptyOperator(task_id="left", outlets=[left])
    c = EmptyOperator(task_id="center", outlets=[])
    r = EmptyOperator(task_id="right", outlets=[right])
    BranchPythonOperator(task_id="decide", python_callable=lambda: ["left", "center"]) >> [l, c, r]


branch_empty()
