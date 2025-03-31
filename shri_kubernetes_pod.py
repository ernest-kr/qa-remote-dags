from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

# Define the DAG
with DAG('kubernetes_pod_operator_example',
         default_args={'owner': 'airflow', 'start_date': datetime(2025, 3, 26)},
         schedule_interval='@once',
         catchup=False) as dag:

    # Task to run in a Kubernetes pod
    run_in_pod = KubernetesPodOperator(
        task_id='run_in_kubernetes_pod',
        namespace='default',  # The Kubernetes namespace where the pod will be created
        image='python:3.8-slim',  # Container image to use
        cmds=["python", "-c", "print('Hello from Kubernetes pod!')"],  # Command to run
        labels={"purpose": "test"},  # Labels for the pod
        name="airflow-test-pod",  # Name of the pod
        is_delete_operator_pod=True,  # Delete the pod after task completion
        get_logs=True,  # Enable logs to be captured
        resources={'request_memory': '1Gi', 'request_cpu': '2Gi'},  # Resource requests
        in_cluster=True  # Use in-cluster configuration if running inside Kubernetes
    )

    run_in_pod
