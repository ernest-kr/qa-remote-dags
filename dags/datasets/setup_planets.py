import json
from datetime import datetime
from pathlib import Path
from textwrap import dedent

from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

test_name = "planets"
postgres_ip_key = "postgres_ip"
postgres_password = "postgres"

# filesystem
csv_content = dedent(
    """
    planet,mass
    MERCURY,0.330
    VENUS,4.87
    EARTH,5.97
    MARS,0.642
    JUPITER,1898
    SATURN,568
    URANUS,86.8
    NEPTUNE,102
    """  # in 10^24kg
).strip()
csv_path = Path(f"/tmp/{test_name}/csv")

# postgres
conn_id = test_name + "_postgres"

# kubernetes
namespace_if_not_set = test_name.replace("_", "-")


def postgres_connection(deploy_data):
    ip = deploy_data.pop("ip")
    return Connection(
        conn_id=conn_id,
        conn_type="postgres",
        host=ip,
        login="postgres",
        schema="postgres",
        password="postgres",
        description=json.dumps(deploy_data),
    )


# called by the setup dag (relevant only if the worker filesystem persists between tasks)
# and during Dockerfile build (for everything else)
@task
def place_file():
    print(f"writing csv to {csv_path}")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(csv_path, "w") as f:
            f.write(csv_content)
    except PermissionError as ex:
        # if the file already exists and is not writable, that's fine
        # assume it was placed there during image build and is correct
        print(ex)


# called by the setup dag
def attempt_connection(
    setup_tables_taskid,
    deploy_postgres_taskid,
    deploy_data={"ip": "0.0.0.0", "namespace": None, "pod": None},
    fail_if_not_found=False,
):
    def pick_next_task(conn):
        "Test the connection, delete or save it, and pick a branch or fail accordingly"
        print("trying connection:", conn)

        result = conn.test_connection()
        print(result)
        if result[0]:
            print("connection test succeeded, moving on to set up database")
            return setup_tables_taskid
        else:
            print("connection test failed")
            if fail_if_not_found:
                # if the IP is not local, then we must have deployed it
                # if we deployed it and can't talk to it, hen we're out of options
                raise AirflowException(f"failed to establish database connection using: {conn.__dict__}")

            print("deleting connection")
            session = Session()
            session.delete(conn)
            session.commit()

            print("moving on to deploy database")
            return deploy_postgres_taskid

    # first see if there's already a conection with this id
    try:
        conn = BaseHook.get_connection(conn_id)
        print("connection already exists")
        return pick_next_task(conn)

    # if not, create a new one
    except AirflowNotFoundException:
        print(f"connection does not already exist, adding it with {deploy_data}")
        if type(deploy_data) is not dict:
            deploy_data = json.loads(deploy_data)
        conn = postgres_connection(deploy_data)
        session = Session()
        session.add(conn)
        session.commit()
        return pick_next_task(conn)


with DAG(dag_id="setup_planets", schedule=None, start_date=datetime(1970, 1, 1), tags=["datasets_neg"]) as setup:
    file_placed = place_file()
    postgres_setup_done = EmptyOperator(task_id="postgres_setup_done", trigger_rule=TriggerRule.ONE_SUCCESS)
    skip_deploy = EmptyOperator(task_id="skip_deploy")

    # can't use KubernetesPodOperator because namespace is not a templated field
    postgres_deployed = BashOperator(
        task_id="deploy_postgres",
        bash_command=dedent(
            f"""
            set -euox pipefail

            # which namespace to use, create it if missing
            if [ -f /etc/podinfo ]
            then
                NAMESPACE=$(cat /etc/podinfo | jq '.namespace')
            else
                kubectl delete namespace {namespace_if_not_set} || true
                kubectl create namespace {namespace_if_not_set}
                NAMESPACE={namespace_if_not_set}
            fi

            # deploy postgres, put IP in XCom
            cat <<- EOF | kubectl -n $NAMESPACE apply -f -
            apiVersion: v1
            kind: Pod
            metadata:
                name: postgres
            spec:
                containers:
                    - name: postgres
                      image: docker.io/postgres
                      env:
                      - name: POSTGRES_PASSWORD
                        value: postgres
                restartPolicy: OnFailure
            EOF
            kubectl -n $NAMESPACE wait --for=condition=ready pod postgres
            IP=$(kubectl -n $NAMESPACE get pod postgres -o json | jq -r '.status.podIP')
            jq -c --null-input --arg IP "$IP" '{{"namespace": "{namespace_if_not_set}", "pod": "postgres", "ip": $IP }}'
            """
        ).strip(),
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # maybe it works the first time because the user is running postgres in docker
    first_connection_attempted = BranchPythonOperator(
        task_id="attempt_connect",
        python_callable=attempt_connection,
        op_args=[skip_deploy.task_id, postgres_deployed.task_id],
    )
    first_connection_attempted >> skip_deploy

    # maybe we have to deploy our own postgres pod
    second_connection_attepted = PythonOperator(
        task_id="attempt_connect_again",
        python_callable=attempt_connection,
        op_args=["unused", "unused"],
        op_kwargs={
            "deploy_data": XComArg(postgres_deployed),
            "fail_if_not_found": True,
        },
    )
    first_connection_attempted >> postgres_deployed >> second_connection_attepted

    # either way, finish up by initializing tables
    [skip_deploy, second_connection_attepted] >> postgres_setup_done

# call this in the dockerfile
if __name__ == "__main__":
    place_file.function()


@task
def get_namespace():
    namespace = None
    try:
        conn = BaseHook.get_connection(conn_id)
        try:
            namespace = json.loads(conn.description)["namespace"]
        except KeyError:
            pass
    except AirflowNotFoundException:
        pass
    return namespace


def is_teardown_needed(namespace, teardown_task_id):
    if namespace:
        return teardown_task_id
    else:
        return None


@task
def delete_connection():
    conn = BaseHook.get_connection(conn_id)
    session = Session()
    session.delete(conn)
    session.commit()


with DAG(dag_id="teardown_planets", schedule=None, start_date=datetime(1970, 1, 1), tags=["datasets_neg"]) as teardown:
    # if we didn't deploy postrges, the connection won't have a "namespace" extra
    deployed_namespace = get_namespace()

    # if we did, deleting that namespace will clean it up
    delete_namespace = BashOperator(
        task_id="delete_namespace",
        bash_command="kubectl delete namespace $NAMESPACE",
        env={"NAMESPACE": deployed_namespace},
        append_env=True,
    )

    # decide which of these cases is relevant
    (
        BranchPythonOperator(
            task_id="is_teardown_needed",
            python_callable=is_teardown_needed,
            op_args=[deployed_namespace, delete_namespace.task_id],
        )
        >> delete_namespace
        >> delete_connection()
    )
