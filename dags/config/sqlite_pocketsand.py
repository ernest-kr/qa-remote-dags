from datetime import datetime
from textwrap import dedent, indent

from airflow.decorators import dag
from plugins.sqlite_pocketsand import (
    conn_id,
    create_connection,
    dockerfile_fragment,
)


@dag(
    dag_id="sqlite_pocketsand",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["setup"],
    doc_md=dedent(
        f"""
        This DAG creates a sqlite connection with id: {conn_id}
        Then it makes sure that we can talk to that DB (it will fail if we can't)
        Run this before you run any dags that depend on that connection.

        For the connection to work you'll also need to have configured a sqlite db
        (perhaps while building the image):

        {indent(dockerfile_fragment, "    ")}
        """
    ),
)
def setup():
    create_connection()


# test_connection(t1)


the_dag = setup()
