from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.sdk import DAG

## kinldy update config_file and cluster_context as per your env
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

from airflow.decorators import task

namespace = conf.get("kubernetes_executor", "NAMESPACE")

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None


dag = DAG(
    "setup_teardown_kubernetes_pod",
    schedule="@once",
    default_args=default_args,
    tags=["setup_teardown"],
)


with dag:
    s = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        config_file=config_file,
        in_cluster=in_cluster,
        is_delete_operator_pod=True,
        get_logs=True,
    ).as_setup()

    @task
    def work():
        print("work")

    t = KubernetesPodOperator(
        namespace=namespace,
        image="alpine",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-two",
        cmds=[
            "sh",
            "-c",
            "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
        ],
        config_file=config_file,
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=True,
    ).as_teardown(setups=s, on_failure_fail_dagrun=True)

    w = work()

    s >> w >> t
