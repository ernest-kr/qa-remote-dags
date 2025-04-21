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


volume_mount = k8s.V1VolumeMount(name="test-volume", mount_path="/root/mount_file", sub_path=None, read_only=True)

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-1")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-2")),
]

volume = k8s.V1Volume(
    name="test-volume",
    empty_dir=k8s.V1EmptyDirVolumeSource(),
)

port = k8s.V1ContainerPort(name="http", container_port=80)

init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path="/etc/foo", name="test-volume", sub_path=None, read_only=True)
]

init_environments = [
    k8s.V1EnvVar(name="key1", value="value1"),
    k8s.V1EnvVar(name="key2", value="value2"),
]

init_container = k8s.V1Container(
    name="init-container",
    image="ubuntu:16.04",
    env=init_environments,
    volume_mounts=init_container_volume_mounts,
    command=["bash", "-cx"],
    args=["echo 10"],
)


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="k8_pod_volume_mount",
    default_args=default_args,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["kpo"],
) as dag:
    k = KubernetesPodOperator(
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        ports=[port],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_from=configmaps,
        name="airflow-mount-volume",
        task_id="task-volume-mount",
        init_containers=[init_container],
    )
