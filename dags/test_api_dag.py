from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

dag = DAG(
    "test_api_dag",
    schedule="@daily",
    catchup=False,
)

hello_task = BashOperator(
    task_id="test_task",
    bash_command='echo "Hello World from Airflow!"',
    do_xcom_push=True,
    dag=dag,
)

hello_task
