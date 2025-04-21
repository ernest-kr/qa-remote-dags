import subprocess

from airflow.configuration import conf
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

namespace = conf.get("kubernetes_executor", "NAMESPACE")
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "verify_astro_pod_mutation",
    default_args=default_args,
    schedule=None,
    start_date=datetime(1970, 1, 1),
    tags=["astro_pod_mutation_hook"],
)


def get_pod_name(regex, ti):
    # Run the command to retrieve the pod name
    result = subprocess.run(["kubectl", "get", "pods", "-o", "name", "-n", namespace], capture_output=True, text=True)
    test_pod_name = ""
    lines = result.stdout.split("\n")
    for line in lines:
        if line.startswith("pod/"):
            pod_name = line.split("pod/")[1]
            if regex in pod_name:
                test_pod_name = pod_name
    # Store the pod name in an XCom variable
    ti.xcom_push(key="return_value", value=test_pod_name)


def get_pod_specs(ti):
    # Run the command to retrieve the pod name
    get_pod_name = ti.xcom_pull(task_ids="get_pod_name")
    pod_specs = subprocess.run(
        ["kubectl", "get", "pod", get_pod_name, "-o", "json", "-n", namespace], capture_output=True, text=True
    ).stdout
    # Store the pod name in an XCom variable
    ti.xcom_push(key="return_value", value=pod_specs)


def verify_astro_pod_mutation_hook(ti):
    import json

    pod_specs = ti.xcom_pull(task_ids="get_pod_specs")
    if not pod_specs:
        raise ValueError("No value currently stored in XComs.")
    pod_specs = json.loads(pod_specs)
    print(pod_specs)

    node_selector_value = pod_specs["spec"]["nodeSelector"]["astronomer.io/node-group"]
    tolerations_value = pod_specs["spec"]["tolerations"][0]["value"]
    assert node_selector_value == "airflow-system"
    assert tolerations_value == "airflow-system"
    assert pod_specs["metadata"]["labels"]["airflow-deployment"] == namespace
    assert pod_specs["metadata"]["name"] == ti.xcom_pull(task_ids="get_pod_name")


verify_pod_mutation_hook = PythonOperator(
    task_id="verify_pod_mutation_hook", python_callable=verify_astro_pod_mutation_hook, dag=dag
)


get_pod_specs = PythonOperator(task_id="get_pod_specs", python_callable=get_pod_specs, dag=dag)

get_pod_name = PythonOperator(task_id="get_pod_name", python_callable=get_pod_name, op_args=[r"scheduler"], dag=dag)


get_pod_name >> get_pod_specs >> verify_pod_mutation_hook
