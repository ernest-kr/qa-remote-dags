from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowRuntimeError
from plugins import api_utility

docs = """
####Purpose
The purpose of this dag is to test that strings instead of key value pairs can be passed to the Connection class extra parameter.\n
It achieves this test by making an assertion that the value passed in is a stringified value.
####Expected Behavior
This dag has 2 tasks in it both of which are expected to succeed.\n
The first task sets up a fake connection for testing purposes.\n
The second task makes an assertion that the value of the extra parameter is the same as what was passed in by using the Connection().get_connection_from_secrets() method.
"""

dag_name = "test_uri_extra_str_data"


def add_conn():
    try:
        BaseHook().get_connection(f"{dag_name}_connection")
        print("The connection has been made previously.")
    except AirflowRuntimeError:
        request_body = {
            "connection_id": f"{dag_name}_connection",
            "conn_type": "stuff",
            "description": "stuff",
            "host": "astronomer.io",
            "login": "Neo",
            "schema": "mathematics",
            "port": 33215,
            "password": "The_ReadPill",
            "extra": '{"a": "10"}',
        }
        response = api_utility.create_connection(request_body)
        assert response.json()["connection_id"] == f"{dag_name}_connection"


def check_uri_gen():
    try:
        c = Connection()
        conn = c.get_connection_from_secrets(f"{dag_name}_connection")
        print("An assert is being made below that the extra parameter is of type string")
        assert isinstance(conn.extra, str)
        print(f"The metadata passed to extra that is not in key:value format: {conn.extra}")
        print(
            "An assert is being made below that the extra parameter of the Connection class is being stored correctly"
        )
        assert conn.extra == '{"a": "10"}'
    except AirflowRuntimeError:
        print("There is no connection to pull data from.")
    finally:
        pass


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
        task_id="check_uri_extra_arbitrary_data",
        python_callable=check_uri_gen,
    )


t0 >> t1
