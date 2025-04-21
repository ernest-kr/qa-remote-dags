import os

from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowRuntimeError
from pendulum import today
from plugins import api_utility

docs = """
####Purpose
This dag tests the template '{ conn.conn_id.connection_param }' to ensure you can grab various items with the template
####Expected Behavior
This dag has 2 tasks both of which are expected to succeed. If one or both of the tasks fail then something is wrong with the jinja 'conn' template.\n
The 1st task defines a table with Postgres with the random connection ID.\n
The 2nd task checks to make sure that the connection values passed in stay the same.
"""

dag_name = "test_jinja_connection_id"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "database-1.cxmxicvi57az.us-east-2.rds.amazonaws.com")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "READ_FROM_ENV")


def add_conn():
    try:
        found = BaseHook().get_connection(f"{dag_name}_connection")
    except AirflowRuntimeError:
        found = None
        print("The connection is not defined, please add a connection in the dags first task")
    if found:
        print("The connection has been made previously.")
    else:
        request_body = {
            "connection_id": f"{dag_name}_connection",
            "conn_type": "postgres",
            "description": "postgres",
            "host": POSTGRES_HOST,
            "login": "postgres",
            "schema": "postgres",
            "port": 5432,
            "password": POSTGRES_PASS,
            "extra": "{}",
        }
        response = api_utility.create_connection(request_body)
        assert response.json()["connection_id"] == f"{dag_name}_connection"


def conn_id_test(**context):
    print(f"The connection type is: {context['get_conn_type']}")
    print(f"The host is: {context['check_host']}")
    print(f"The schema is: {context['get_schema']}")
    print(f"The login is: {context['get_login']}")
    print(f"The password is: {context['get_pass']}")  # returns '***' hides the password
    print(f"The port is: {context['get_port']}")
    print(f"The extras are: {context['get_extras']}")

    print("asserting the connection type")
    assert context["get_conn_type"] == "postgres"
    print("asserting the host")
    assert context["check_host"] == POSTGRES_HOST
    print("asserting the schema")
    assert context["get_schema"] == "postgres"
    print("asserting the login")
    assert context["get_login"] == "postgres"
    print("asserting the port")
    assert context["get_port"] == "5432"
    # print("asserting the extras")
    # assert context["get_extras"] == "{'key': 'value'}"


with DAG(
    dag_id=dag_name,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    doc_md=docs,
    tags=["core"],
) as dag:
    py0 = PythonOperator(
        task_id="add_conn",
        python_callable=add_conn,
    )

    P0 = SQLExecuteQueryOperator(
        task_id="create_table_define_cols",
        conn_id=f"{dag_name}_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS jinja_connection_template_test(
            random_str varchar,
            herbs varchar,
            primary key(herbs));
            """,
    )

    py1 = PythonOperator(
        task_id="check_jinja_conn_id",
        python_callable=conn_id_test,
        op_kwargs={
            "get_conn_type": f"{{{{ conn.{dag_name}_connection.conn_type }}}}",
            "check_host": f"{{{{ conn.{dag_name}_connection.host }}}}",
            "get_schema": f"{{{{ conn.{dag_name}_connection.schema }}}}",
            "get_login": f"{{{{ conn.{dag_name}_connection.login }}}}",
            "get_pass": f"{{{{ conn.{dag_name}_connection.password }}}}",
            "get_port": f"{{{{ conn.{dag_name}_connection.port }}}}",
            "get_extras": f"{{{{ conn.{dag_name}_connection.extra }}}}",
        },
    )


py0 >> P0 >> py1
