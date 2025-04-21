from datetime import datetime

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.sdk import DAG

namespace = conf.get("kubernetes_executor", "NAMESPACE")

with DAG(
    dag_id="kpo_mapped",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["taskmap"]
    # render_template_as_native_obj=True,
) as dag:
    KubernetesPodOperator(
        task_id="cowsay_static",
        name="cowsay_statc",
        namespace=namespace,
        image="docker.io/rancher/cowsay",
        cmds=["cowsay"],
        arguments=["moo"],
        log_events_on_failure=True,
    )

    KubernetesPodOperator.partial(
        task_id="cowsay_mapped",
        name="cowsay_mapped",
        namespace=namespace,
        image="docker.io/rancher/cowsay",
        cmds=["cowsay"],
        log_events_on_failure=True,
    ).expand(arguments=[["mooooove"], ["cow"], ["get out the way"]])
