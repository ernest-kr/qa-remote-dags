from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s


def call_me():
    print("hello")


with DAG(
    dag_id="k8s_pod_req_negative",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["k8s_exe_neg"]
    # render_template_as_native_obj=True,
) as dag:
    sp = PythonOperator(
        task_id="override_request",
        python_callable=call_me,
        dag=dag,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    "cpu": "1002138625498m",
                                    "memory": "500000000000Mi",
                                },
                                limits={
                                    "cpu": "1",
                                    "memory": "10G",
                                },
                            ),
                        )
                    ]
                )
            )
        },
    )
