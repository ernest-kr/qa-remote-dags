import subprocess
import time
from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

namespace = conf.get("kubernetes_executor", "NAMESPACE")

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kpo_override_resource_negative_case",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    tags=["core", "extended_dagsk8", "k8s-pod"],
)


def get_pod_name(regex_list, ti):
    # Run the command to retrieve the pod name
    time.sleep(15)
    result = subprocess.run(["kubectl", "get", "pods", "-o", "name", "-n", namespace], capture_output=True, text=True)
    print(f"result is {result}")
    for regex in regex_list:
        test_pod_name = ""
        lines = result.stdout.split("\n")
        for line in lines:
            if line.startswith("pod/"):
                pod_name = line.split("pod/")[1]
                if regex in pod_name:
                    test_pod_name = pod_name
        # Store the pod name in an XCom variable
        ti.xcom_push(key=regex, value=test_pod_name)


def get_pod_specs(regex_list, ti):
    # Run the command to retrieve the pod name
    for regex in regex_list:
        get_pod_name = ti.xcom_pull(task_ids="get_pod_name_task", key=regex)
        print(f"pod name is {get_pod_name}")
        pod_specs = subprocess.run(
            ["kubectl", "get", "pod", get_pod_name, "-o", "json", "-n", namespace], capture_output=True, text=True
        ).stdout
        # Store the pod name in an XCom variable
        ti.xcom_push(key=regex, value=pod_specs)


def verify_kpo_pod_specs(regex_list, ti):
    import json

    failures = []
    for regex in regex_list:
        pod_specs = ti.xcom_pull(task_ids="get_pod_specs_task", key=regex)
        if not pod_specs:
            raise ValueError("No value currently stored in XComs.")
        pod_specs = json.loads(pod_specs)
        print(pod_specs)
        limits = pod_specs["spec"]["containers"][0]["resources"]["limits"]
        requests = pod_specs["spec"]["containers"][0]["resources"]["requests"]
        required_keys = ["cpu", "memory"]  # Keys to check for existence
        for key in required_keys:
            if key not in limits or key not in requests:
                failures.append(f"{key}' is missing for regex {regex}")
    if len(failures) > 0:
        raise Exception(print(*failures, sep=", "))


start = EmptyOperator(task_id="run_this_first", dag=dag)


k8s_resource_requirements_negative_1 = k8s.V1ResourceRequirements(limits={"cpu": "100m", "memory": "256Mi"})

limits_only = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_1,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="limits-only",
    task_id="limits-only",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

k8s_resource_requirements_negative_2 = k8s.V1ResourceRequirements(limits={"memory": "256Mi"})

limits_only_with_empty_cpu = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_2,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="limits-only-with-empty-cpu",
    task_id="limits-only-with-empty-cpu",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

k8s_resource_requirements_negative_3 = k8s.V1ResourceRequirements(limits={"cpu": "100m"})

limits_only_with_empty_mem = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_3,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="limits-only-with-empty-mem",
    task_id="limits-only-with-empty-mem",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

k8s_resource_requirements_negative_4 = k8s.V1ResourceRequirements(requests={"cpu": 1, "memory": "2Gi"})

requests_only = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_4,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="requests_only",
    task_id="requests_only",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

k8s_resource_requirements_negative_5 = k8s.V1ResourceRequirements(limits={"memory": "2Gi"})

requests_only_with_empty_cpu = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_5,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="requests-only-with-empty-cpu",
    task_id="requests-only-with-empty-cpu",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

k8s_resource_requirements_negative_6 = k8s.V1ResourceRequirements(limits={"cpu": 1})

requests_only_with_empty_mem = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    container_resources=k8s_resource_requirements_negative_6,
    image="alpine",
    cmds=[
        "sh",
        "-c",
        "sleep 200",
    ],
    labels={"foo": "bar"},
    name="requests-only-with-empty-mem",
    task_id="requests-only-with-empty-mem",
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
)

verify_kpo_pod_specs_task = PythonOperator(
    task_id="verify_kpo_pod_specs_task",
    python_callable=verify_kpo_pod_specs,
    op_args=[
        [
            "limits-only-with-empty-cpu",
            "limits-only-with-empty-mem",
            "requests-only-with-empty-cpu",
            "requests-only-with-empty-mem",
        ]
    ],
    dag=dag,
)


get_pod_specs_task = PythonOperator(
    task_id="get_pod_specs_task",
    python_callable=get_pod_specs,
    op_args=[
        [
            "limits-only-with-empty-cpu",
            "limits-only-with-empty-mem",
            "requests-only-with-empty-cpu",
            "requests-only-with-empty-mem",
        ]
    ],
    dag=dag,
)

get_pod_name_task = PythonOperator(
    task_id="get_pod_name_task",
    python_callable=get_pod_name,
    op_args=[
        [
            "limits-only-with-empty-cpu",
            "limits-only-with-empty-mem",
            "requests-only-with-empty-cpu",
            "requests-only-with-empty-mem",
        ]
    ],
    dag=dag,
)


limits_only.set_upstream(start)

limits_only_with_empty_cpu.set_upstream(start)
limits_only_with_empty_mem.set_upstream(start)
requests_only.set_upstream(start)
requests_only_with_empty_cpu.set_upstream(start)
requests_only_with_empty_mem.set_upstream(start)
get_pod_name_task.set_upstream(start)
get_pod_specs_task.set_upstream(get_pod_name_task)
verify_kpo_pod_specs_task.set_upstream(get_pod_specs_task)
