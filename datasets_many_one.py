from datetime import datetime

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

fan_out = Dataset("fan_out")
fan_in = Dataset("fan_in")


# the leader
with DAG(dag_id="momma_duck", start_date=datetime(1970, 1, 1), schedule=None, tags=["datasets"]) as leader:
    PythonOperator(task_id="has_outlet", python_callable=lambda: None, outlets=[fan_out])

# the many
for i in range(1, 2):
    with DAG(
        dag_id=f"duckling_{i}", start_date=datetime(1970, 1, 1), schedule=[fan_out], is_paused_upon_creation=False
    ) as duck:
        PythonOperator(task_id="has_outlet", python_callable=lambda: None, outlets=[fan_in])
    globals()[f"duck_{i}"] = duck


# the straggler
with DAG(
    dag_id="straggler_duck", start_date=datetime(1970, 1, 1), schedule=[fan_in], is_paused_upon_creation=False
) as straggler:
    PythonOperator(task_id="has_outlet", python_callable=lambda: None)
