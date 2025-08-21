from datetime import timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook
from airflow.utils.dates import days_ago


def verify_aws_secret(**context):
    """
    Verify that the AWS Secret Manager secret can be accessed.

    This function uses the SecretsManagerHook to retrieve the secret value
    from AWS Secrets Manager and prints it to the logs.
    """
    # Secret name created in the Terraform file
    secret_name = "airflow/test-remote-variable"

    # Initialize the SecretsManagerHook
    hook = SecretsManagerHook(aws_conn_id="aws_default")

    try:
        # Get the secret value
        secret_value = hook.get_secret_value(secret_id=secret_name)

        # Log the secret value (in production, you would not want to log secrets)
        print(f"Successfully retrieved secret: {secret_name}")
        print(f"Secret value: {secret_value}")

        # You can also access the secret as a dictionary if it's in JSON format
        # secret_dict = json.loads(secret_value)

        # Store the result in XCom for potential downstream tasks
        context['ti'].xcom_push(key='secret_verified', value=True)

        return True
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        context['ti'].xcom_push(key='secret_verified', value=False)
        raise


def report_verification_result(**context):
    """
    Report the result of the secret verification.
    """
    # Get the result from XCom
    secret_verified = context['ti'].xcom_pull(task_ids='verify_secret_task', key='secret_verified')

    if secret_verified:
        print("AWS Secret verification was successful!")
    else:
        print("AWS Secret verification failed!")

    return secret_verified


with DAG(
        "verify_aws_secret",
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="A DAG to verify AWS Secrets Manager secret access",
        schedule=None,
        start_date=days_ago(1),
        catchup=False,
        tags=["test", "aws", "secrets"],
) as dag:
    verify_secret = PythonOperator(
        task_id="verify_secret_task",
        python_callable=verify_aws_secret,
        provide_context=True,
    )

    report_result = PythonOperator(
        task_id="report_result_task",
        python_callable=report_verification_result,
        provide_context=True,
    )

    # Set task dependencies
    verify_secret >> report_result