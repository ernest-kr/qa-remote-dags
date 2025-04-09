from airflow.configuration import conf
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

namespace = conf.get("kubernetes", "NAMESPACE")

if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None


default_args = {
    "owner": "airflow",
}
k8s_resource_requirements = k8s.V1ResourceRequirements(
    requests={"cpu": "0.5", "memory": "1Gi"}, limits={"cpu": "0.5", "memory": "1Gi"}
)

with DAG(
    dag_id="5-sec-sleep-kpo-dag",
    default_args=default_args,
    max_active_runs=1,
    catchup=True,
    concurrency=1000000,
    schedule=None,
    start_date=datetime(2022, 8, 10),
) as dag_1:
    long_dag = KubernetesPodOperator(
        namespace=namespace,
        in_cluster=in_cluster,
        container_resources=k8s_resource_requirements,
        config_file=config_file,
        image="alpine",
        cmds=[
            "sh",
            "-c",
            "sleep 5",
        ],
        name="short_sleep",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        task_id="long_sleep",
        get_logs=True,
    )
