import os

from requests import request


def get_env_values():
    """Fetch environment variables dynamically to avoid import-time issues."""
    cloud_ui_url = os.environ.get("AIRFLOW__ASTRONOMER__CLOUD_UI_URL", "")
    api_base_url = os.environ.get("AIRFLOW__API__BASE_URL", "http://localhost:28080")

    bearer_token = "default_value"
    base_url = f"{api_base_url}api/v2"
    print(f"base url is {base_url}")

    if "cloud.astronomer-stage.io" in cloud_ui_url:
        bearer_token = os.environ.get("STAGE_ORG_TOKEN", "default_value")
    elif "cloud.astronomer-dev.io" in cloud_ui_url:
        bearer_token = os.environ.get("DEV_ORG_TOKEN", "default_value")

    return bearer_token, base_url


def create_connection(request_body):
    bearer_token, base_url = get_env_values()
    print(f"token is {bearer_token}")

    url = f"{base_url}/connections"
    print(f"url is {url}")
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    response = request(method="POST", url=url, headers=headers, json=request_body)
    print(f"response is {response.status_code}")
    print(f"response is {response.json()}")
    return response


def delete_connection(connection_id):
    bearer_token, base_url = get_env_values()
    url = f"{base_url}/connections/{connection_id}"
    headers = {"accept": "*/*", "Authorization": f"Bearer {bearer_token}"}
    response = request(method="DELETE", url=url, headers=headers)
    return response


def get_task_instances(dag_id, run_id):
    bearer_token, base_url = get_env_values()
    url = f"{base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    headers = {"accept": "application/json", "Authorization": f"Bearer {bearer_token}"}
    response = request(method="GET", url=url, headers=headers)
    return response


def get_task_instance(dag_id, run_id, task_id):
    bearer_token, base_url = get_env_values()
    url = f"{base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {bearer_token}"}
    response = request(method="GET", url=url, headers=headers)
    return response


def get_task_instance_log(dag_id, run_id, task_id, try_number):
    bearer_token, base_url = get_env_values()
    url = f"{base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}?full_content=true&map_index=-1"
    headers = {"accept": "application/json", "Authorization": f"Bearer {bearer_token}"}
    response = request(method="GET", url=url, headers=headers)
    return response
