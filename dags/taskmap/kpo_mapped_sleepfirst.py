from datetime import datetime

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)
from airflow.sdk import DAG

namespace = conf.get("kubernetes_executor", "NAMESPACE")

with DAG(
    dag_id="kpo_mapped_sleepfirst",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["taskmap"],
    doc_md="for testing reattach_on_restart, restart scheduler before 200 seconds is up and task should complete",
) as dag:
    KubernetesPodOperator(
        task_id="cowsay_static",
        name="cowsay_statc",
        namespace=namespace,
        image="docker.io/rancher/cowsay",
        cmds=["sh"],
        arguments=["-c", "sleep 200 && cowsay moo"],
        reattach_on_restart=True,
        log_events_on_failure=True,
    )

    KubernetesPodOperator.partial(
        task_id="cowsay_mapped",
        name="cowsay_mapped",
        namespace=namespace,
        image="docker.io/rancher/cowsay",
        cmds=["sh"],
        log_events_on_failure=True,
        reattach_on_restart=True,
    ).expand(arguments=[["-c", f"sleep 200 && cowsay {x}"] for x in ["mooooove", "cow", "get out the way"]])
