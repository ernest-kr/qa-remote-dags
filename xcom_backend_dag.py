from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="xcom_backend_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Test DAG to verify S3 XCom backend functionality"
):

    @task()
    def xcom_producer():
        print("Producing value...")
        return "my_value"

    @task()
    def xcom_consumer(value):
        print("Consumed value:", value)
        assert value == "my_value", f"Expected 'my_value' but got {value}"
        print("✅ XCom value verified successfully")

    # Create tasks explicitly
    produced_value = xcom_producer()
    xcom_consumer(produced_value)
