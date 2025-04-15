from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.configuration import conf

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

namespace = "shri-awslogs"

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None

dag = DAG(
    "simple_kubernetes_pod",
    schedule="@once",
    default_args=default_args,
    tags=["kpo"],
)


with dag:
    k = KubernetesPodOperator(
        namespace="shri-awslogs",
        image="hello-world",
        labels={"app": "airflow"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
        config_file=config_file,
        on_finish_action="keep_pod",
        get_logs=True,
        service_account_name="shri-awslogs"
    )
