import os

from airflow.example_dags.libs.helper import print_stuff
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s
from pendulum import today

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="example_kubernetes_executor",
    default_args=args,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["K8_executor"],
) as dag:
    affinity = k8s.V1PodAffinity(
        required_during_scheduling_ignored_during_execution=[
            k8s.V1PodAffinityTerm(
                topology_key="kubernetes.io/hostname",
                label_selector=k8s.V1LabelSelector(
                    match_expressions=[k8s.V1LabelSelectorRequirement(key="app", operator="in", values=["airflow"])]
                ),
            )
        ]
    )

    tolerations = [{"key": "dedicated", "operator": "Equal", "value": "airflow"}]

    def use_zip_binary():
        """
        Checks whether Zip is installed.

        :return: True if it is installed, False if not.
        :rtype: bool
        """
        return_code = os.system("zip")
        if return_code != 0:
            raise SystemError("The zip binary is missing")

    # You don't have to use any special KubernetesExecutor configuration if you don't want to
    start_task = PythonOperator(task_id="start_task", python_callable=print_stuff)

    # But you can if you want to
    one_task = PythonOperator(
        task_id="one_task",
        python_callable=print_stuff,
        executor_config={"KubernetesExecutor": {"image": "ephraimbuddy/newproj:0.1"}},
    )

    # Use the zip binary, which is only found in this special docker image
    two_task = PythonOperator(
        task_id="two_task",
        python_callable=use_zip_binary,
        executor_config={"KubernetesExecutor": {"image": "ephraimbuddy/newproj:0.1"}},
    )

    # Limit resources on this operator/task with node affinity & tolerations
    three_task = PythonOperator(
        task_id="three_task",
        python_callable=print_stuff,
        executor_config={
            "KubernetesExecutor": {
                "request_memory": "128Mi",
                "limit_memory": "128Mi",
                "tolerations": tolerations,
                "affinity": affinity,
            }
        },
    )

    # Add arbitrary labels to worker pods
    four_task = PythonOperator(
        task_id="four_task",
        python_callable=print_stuff,
        executor_config={"KubernetesExecutor": {"labels": {"foo": "bar"}}},
    )

    start_task >> [one_task, two_task, four_task] >> three_task
