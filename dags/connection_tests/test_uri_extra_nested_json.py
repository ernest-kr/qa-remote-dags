from datetime import datetime

from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowRuntimeError

docs = """
####Purpose
The purpose of this dag is to test that nested json can be passed to the Connection class extra parameter.\n
It achieves this test by making an assertion that the value passed in is a stringified value, as json renders it to a string in python
####Expected Behavior
This dag has 2 tasks in it both of which are expected to succeed.\n
The first task sets up a fake connection for testing purposes.\n
The second task makes an assertion that the value of the extra parameter is the same as what was passed in by using the Connection().get_connection_from_secrets() method.
"""

dag_name = "test_uri_extra_nested_JSON"


def add_conn():
    try:
        BaseHook().get_connection(f"{dag_name}_connection")
        print("The connection has been made previously.")
    except AirflowRuntimeError:
        remote_connection = Connection(
            conn_id=f"{dag_name}_connection",
            conn_type="stuff",
            host="astronomer.io",
            login="Neo",
            password="The_ReadPill",
            schema="mathematics",
            port=33307,
            extra={"outer_scope": {"middle_scope": {"inner_scope": [1, "two", 3, "four"]}}},
        )
        print(remote_connection)
        session = settings.Session()
        session.add(remote_connection)
        session.commit()


def check_uri_gen():
    try:
        c = Connection()
        conn = c.get_connection_from_secrets(f"{dag_name}_connection")
        print("An assert is being made below that the extra parameter is of type string")
        assert isinstance(conn.extra, str)
        print("An assert is being made below that the extra parameter is rendered to json correctly")
        assert conn.extra == '{"outer_scope": {"middle_scope": {"inner_scope": [1, "two", 3, "four"]}}}'
    except AirflowRuntimeError:
        print("There is no connection to pull data from.")
    finally:
        # clean up the connection by deleting it
        session = settings.Session()
        session.delete(conn)


with DAG(
    dag_id=dag_name,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    doc_md=docs,
    tags=["core", "connections"],
) as dag:
    t0 = PythonOperator(
        task_id="add_conn",
        python_callable=add_conn,
    )

    t1 = PythonOperator(
        task_id="check_uri_generation",
        python_callable=check_uri_gen,
    )

t0 >> t1
