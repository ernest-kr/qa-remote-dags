from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
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
    dag_id="k8_pod_operator_xcom",
    default_args=default_args,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["kpo"],
) as dag:
    write_xcom = KubernetesPodOperator(
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        image="alpine",
        cmds=[
            "sh",
            "-c",
            "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
        ],
        name="write-xcom",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        task_id="write-xcom",
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )

    write_xcom >> pod_task_xcom_result
