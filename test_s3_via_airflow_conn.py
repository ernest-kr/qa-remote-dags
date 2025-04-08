from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import boto3
import logging
import json

def test_s3_with_airflow_connection():
    conn_id = "MyS3Conn"  # 🔁 Your Airflow connection ID
    bucket = "your-bucket-name"  # 🔁 Replace with your actual bucket

    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson

    session = boto3.session.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        aws_session_token=extra.get("aws_session_token"),
        region_name=extra.get("region_name", "us-east-1"),
    )

    s3 = session.client("s3")

    try:
        logging.info("✅ Connected to S3 using Airflow connection.")
        # List objects
        result = s3.list_objects_v2(Bucket=bucket)
        logging.info(f"📝 Found {len(result.get('Contents', []))} objects in bucket.")

        # Upload test file
        s3.put_object(Bucket=bucket, Key="airflow-test/test_from_connection.txt", Body="Hello from Airflow!")
        logging.info("✅ Successfully uploaded a test file.")
    except Exception as e:
        logging.error(f"❌ S3 access failed: {str(e)}")
        raise

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="test_s3_via_airflow_conn",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["test", "s3", "connection"],
) as dag:
    test_task = PythonOperator(
        task_id="check_s3_conn",
        python_callable=test_s3_with_airflow_connection,
    )






