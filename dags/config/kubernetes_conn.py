from datetime import datetime
from textwrap import dedent

from airflow.decorators import dag
from plugins.k8s_default_conn import (
    conn_id,
    create_connection,
)


@dag(
    dag_id="kubernetes_conn",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["setup"],
    doc_md=dedent(
        f"""
        This DAG creates a K8s connection with id: {conn_id}

        """
    ),
)
def setup():
    create_connection()


the_dag = setup()
