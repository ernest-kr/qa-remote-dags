from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

with DAG(
    dag_id="resource",
    catchup=False,
    schedule="@once",
    start_date=datetime(2020, 1, 1),
) as dag:
    op = BashOperator(
        task_id="task",
        bash_command="sleep 10",
        dag=dag,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    #     "cpu": 100,
                                    "memory": "1Gi",
                                },
                                # limits={"cpu": 200, "memory": "2Gi"},
                            ),
                        )
                    ]
                )
            )
        },
    )
