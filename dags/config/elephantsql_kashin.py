from datetime import datetime
from textwrap import dedent

from airflow.decorators import dag
from plugins.elephantsql_kashin import conn_id, create_connection, test_connection


@dag(
    dag_id="elephantsql_kashin",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["setup"],
    doc_md=dedent(
        f"""
        This DAG creates a postgres connection with id: {conn_id}
        Then it makes sure that we can talk to that DB (it will fail if we can't)

        Run this before you run any dags that depend on that connection
        """
    ),
)
def setup():
    t1 = create_connection()
    test_connection(t1)


the_dag = setup()
