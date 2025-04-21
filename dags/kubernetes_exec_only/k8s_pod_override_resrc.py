from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.settings import conf
from kubernetes import client, config
from kubernetes.client import models as k8s


def call_me():
    print("hello")


def print_pod_name(**context):
    dg = context["dag_run"]
    t = context["task"].task_id
    print("TASK_ID IS ", t)
    cti = [i for i in dg.get_task_instances() if i.task_id == t][0]
    print("FILTERED TASKINSTANCE OBJECT", cti)
    print("POD NAME : ", cti.hostname)
    pod_name = cti.hostname
    # Load the Kubernetes configuration
    config.load_kube_config()

    # Create a Kubernetes API client
    api = client.CoreV1Api()

    namespace = conf.get("kubernetes_executor", "NAMESPACE")
    # Define the Pod's namespace and name
    namespace = namespace
    name = pod_name

    # Get the Pod's details
    pod = api.read_namespaced_pod(name, namespace)

    # Convert the Pod's details to a dictionary and add the 'kubectl describe' output to it
    pod_dict = pod.to_dict()
    pod_dict["describe_output"] = api.read_namespaced_pod(
        name, namespace, _preload_content=False, pretty=True
    ).data.decode("utf-8")

    print("pod yaml", pod_dict)


with DAG(
    dag_id="k8s_executor_config",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    # render_template_as_native_obj=True,
) as dag:
    sp = PythonOperator(
        task_id="override",
        python_callable=print_pod_name,
        dag=dag,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": 0.5, "memory": "500Mi", "ephemeral-storage": "1Gi"},
                                limits={"cpu": 0.5, "memory": "500Mi", "ephemeral-storage": "1Gi"},
                            ),
                        )
                    ]
                )
            )
        },
    )
