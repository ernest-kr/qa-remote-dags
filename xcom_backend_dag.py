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
        return "my_value"

    @task()
    def xcom_consumer(value):
        assert value == "my_value"
        print("✅ XCom value verified successfully:", value)

    xcom_consumer(xcom_producer())  # type: ignore
