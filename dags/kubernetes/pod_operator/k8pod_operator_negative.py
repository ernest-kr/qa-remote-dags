from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

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
}

dag = DAG(
    "k8pod_operator_negative",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    tags=["neg_kpo"],
)


start = EmptyOperator(task_id="run_this_first", dag=dag)

passing = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    image="python:latest",
    cmds=["python", "-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    get_logs=True,
    dag=dag,
)

failing = KubernetesPodOperator(
    namespace=namespace,
    in_cluster=in_cluster,
    config_file=config_file,
    image="ubuntu:latest",
    cmds=["Python", "-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="fail",
    task_id="failing-task",
    get_logs=True,
    dag=dag,
)


passing.set_upstream(start)
failing.set_upstream(start)
