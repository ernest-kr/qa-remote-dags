from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import numpy as np

def memory_stress_test():
    _ = [np.zeros((10000, 10000)) for _ in range(100)]

with DAG(
    "memory_oom_test",
    start_date=datetime(2024, 4, 1),
    catchup=False,
    schedule=None,
    max_active_tasks=50,  # Set high concurrency
) as dag:

    tasks = [
        PythonOperator(
            task_id=f"memory_task_{i}",
            python_callable=memory_stress_test
        )
        for i in range(50)  # Run 50 memory-heavy tasks in parallel
    ]
