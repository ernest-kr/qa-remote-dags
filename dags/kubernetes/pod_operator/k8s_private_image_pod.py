from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.sdk import DAG
from kubernetes.client import models as k8s
from pendulum import today

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
}

with DAG(
    dag_id="k8_pod_private_image",
    default_args=default_args,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["kpo"],
) as dag:
    quay_k8s = KubernetesPodOperator(
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        image="dgastronomer/privateimage:v2",
        image_pull_secrets=[k8s.V1LocalObjectReference("regcred")],
        cmds=["bash", "-cx"],
        arguments=["echo", "10", "echo pwd"],
        labels={"foo": "bar"},
        name="airflow-private-image-pod",
        is_delete_operator_pod=False,
        task_id="secret-pull",
        get_logs=True,
    )

    quay_k8s
