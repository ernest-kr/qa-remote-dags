from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import json
import os
from datetime import timedelta


class S3XComBackend:
    @staticmethod
    def set(key, value, execution_date=None, task_instance=None, session=None):
        # Serialize the value
        serialized_value = json.dumps(value)
        
     
        # Create an S3 hook using the default AWS connection
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Load the string to the S3 bucket
        s3_hook.load_string(serialized_value, s3_key, bucket_name=s3_bucket, replace=True)

    @staticmethod
    def get(key, execution_date=None, task_instance=None, session=None):
        # Define the S3 bucket and key for retrieving the XCom value
        s3_key = f"xcom/{task_instance.dag_id}/{task_instance.task_id}/{execution_date.isoformat()}/{key}"
        s3_bucket = os.getenv("AIRFLOW_XCOM_BUCKET")
        
        # Create an S3 hook using the default AWS connection
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # Read the key from the S3 bucket
        xcom_value = s3_hook.read_key(s3_key, s3_bucket)
        
        # Deserialize and return the value
        return json.loads(xcom_value)

# Define the DAG
with DAG(
    'custom_xcom_s3_backend',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='A simple DAG using a custom S3 XCom backend',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task to push data to XCom
    @task(task_id='push_xcom_to_s3')
    def push_xcom():
        # Push a sample value to XCom
        sample_data = {"message": "This is a test message from Airflow!"}
        print(f"Pushing XCom value: {sample_data}")
        
        # Use the custom S3 XCom backend to store the value
        S3XComBackend.set(key="test_message", value=sample_data, task_instance=task_instance)
    
    # Task to pull data from XCom
    @task(task_id='pull_xcom_from_s3')
    def pull_xcom():
        # Retrieve the value from XCom using the custom backend
        retrieved_value = S3XComBackend.get(key="test_message", task_instance=task_instance)
        print(f"Retrieved XCom value: {retrieved_value}")
        return retrieved_value
    
    # Task dependencies
    push_xcom_task = push_xcom()
    pull_xcom_task = pull_xcom()
    
    push_xcom_task >> pull_xcom_task
