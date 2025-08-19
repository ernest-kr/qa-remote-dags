import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_aws_secrets_manager',
    default_args=default_args,
    description='Test AWS Secrets Manager integration',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


def get_secret():
    """Get secret from AWS Secrets Manager and validate it."""
    secret_name = os.environ.get('TEST_SECRET_NAME', '{secret_name}')
    region_name = os.environ.get('AWS_REGION', 'us-east-1')
    expected_value = '{expected_value or "secret-value"}'

    print(f"Accessing AWS Secret: {{secret_name}}")

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

        # Check if the secret exists and has content
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            print(f"Successfully retrieved secret from AWS Secrets Manager")

            # Parse the JSON secret
            secret_dict = json.loads(secret)

            # Validate the secret value if expected_value is provided
            if expected_value and 'test_key' in secret_dict:
                actual_value = secret_dict['test_key']
                print(f"Validating secret value...")

                if actual_value == expected_value:
                    print("✓ Secret value matches expected value")
                else:
                    print(f"❌ Secret value mismatch! Expected: {{expected_value}}, Got: {{actual_value}}")
                    return False

            return True
        else:
            print(f"Secret exists but has no string value")
            return False

    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Error retrieving secret: {{error_code}}")
        raise e


# Create task to get secret
task_get_secret = PythonOperator(
    task_id='get_aws_secret',
    python_callable=get_secret,
    dag=dag,
)
