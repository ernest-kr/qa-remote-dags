from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to push data to XCom
def push_xcom_value(**kwargs):
    # Push a value to XCom
    kwargs['ti'].xcom_push(key='my_key', value='Hello from Task 1')

# Function to pull data from XCom
def pull_xcom_value(**kwargs):
    # Pull the value from XCom
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='task_1', key='my_key')
    print(f'Retrieved value from XCom: {value}')

# Define the DAG
with DAG(
    'xcom_example_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 3, 26),
        'retries': 1,
    },
    description='An example DAG using XCom', # Runs only manually for this example
    catchup=False,
) as dag:

    # Task 1: Push a value to XCom
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=push_xcom_value,
    )

    # Task 2: Pull the value from XCom
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=pull_xcom_value,
    )

    # Set task dependencies
    task_1 >> task_2
