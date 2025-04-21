from airflow.decorators import task
from airflow.hooks.base import BaseHook
from plugins import api_utility

# take extra care when iterating on this file through astro
# just because you updated a plugin import doesn't mean that airflow has noticed the change
# see: https://astronomer.slack.com/archives/CGQSYG25V/p1643236770299700 for more


conn_id = "kubernetes_default"
conn_type = "Kubernetes Cluster Connection"


@task
def create_connection():
    try:
        conn = BaseHook.get_connection(conn_id)
        print(f"Found: {conn}")
        # assuming that if it has the connection id we expect, it also has the contents that we expect
    except Exception:
        request_body = {"connection_id": conn_id, "conn_type": conn_type}
        response = api_utility.create_connection(request_body)
        assert response.json()["connection_id"] == conn_id
