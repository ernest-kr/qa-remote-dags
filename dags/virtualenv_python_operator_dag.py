from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s
from pendulum import today


def callable_virtualenv():
    """
    Example function that will be performed in a virtual environment.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep

    from colorama import Back, Fore, Style

    print(Fore.RED + "some red text")
    print(Back.GREEN + "and with a green background")
    print(Style.DIM + "and in dim text")
    print(Style.RESET_ALL)
    for _ in range(10):
        print(Style.DIM + "Please wait...", flush=True)
        sleep(10)
    print("Finished")


with DAG(
    dag_id="virtualenv_python_operator",
    default_args={"owner": "airflow"},
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["core"],
) as dag:
    task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["colorama==0.4.0"],
        system_site_packages=False,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    "cpu": "100m",
                                    "memory": "384Mi",
                                },
                                limits={
                                    "cpu": 1,
                                    "memory": "500Mi",
                                },
                            ),
                        )
                    ]
                )
            )
        },
    )
