from datetime import datetime, timedelta

from airflow.configuration import conf
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

from airflow.decorators import setup, task, teardown

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
    "setup_teardown_kubernetes_pod_taskflow",
    schedule="@once",
    default_args=default_args,
    tags=["setup_teardown"],
)


with dag:

    @setup
    @task.kubernetes(
        image="python:3.8-slim-buster",
        name="k8s_test",
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
    )
    def execute_in_k8s_pod_setup():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    @task
    def work():
        print("work")

    @teardown
    @task.kubernetes(
        image="python:3.8-slim-buster",
        name="k8s_test",
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
    )
    def execute_in_k8s_pod_teardown():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    s = execute_in_k8s_pod_setup()
    w = work()
    t = execute_in_k8s_pod_teardown()
    with s >> t as context:
        context.add_task(w)

#    s >> w >> t.as_teardown(setups=s)
