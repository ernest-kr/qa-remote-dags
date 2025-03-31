from airflow.models import DAG
from pendulum import today
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

def print_test():
    print("test")

# Common DAG arguments
args = {
    "owner": "airflow",
    "start_date": today('UTC').add(days=-2),
}

def create_dag(dag_id):
    with DAG(
        dag_id=dag_id,
        default_args=args,
        max_active_runs=3,
        schedule="@hourly"
    ) as dag:
        tasks = [
            PythonOperator(
                task_id=f"tasks_{i}_of_50",
                python_callable=print_test,
            )
            for i in range(1, 51)
        ]
        chain(*tasks)
    return dag

# Creating multiple DAGs dynamically
dag_ids = [f"thput__065_{i}" for i in range(1, 21)]

globals().update({dag_id: create_dag(dag_id) for dag_id in dag_ids})