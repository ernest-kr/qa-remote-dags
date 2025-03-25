from airflow import Dataset
from datetime import datetime
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

dataset = Dataset("dataset")


@dag(start_date=datetime(1970, 1, 1), )
def upstream():
    EmptyOperator(task_id="empty", outlets=[dataset])


upstream()


@dag(start_date=datetime(1970, 1, 1), schedule=[dataset], is_paused_upon_creation=False)
def downstream():
    EmptyOperator(task_id="empty")


downstream()
