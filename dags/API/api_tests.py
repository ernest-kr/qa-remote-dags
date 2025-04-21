import json
import os
import subprocess

from airflow.decorators import task
from airflow.sdk import DAG
from pendulum import today

with DAG(dag_id="api_tests", start_date=today("UTC").add(days=-1), schedule=None, catchup=False, tags=["api"]) as dag:
    dag_dir = os.path.dirname(__file__)
    postman_collection = os.path.join(dag_dir, "airflow3_api_collection.json")
    postman_collection_updated = os.path.join(dag_dir, "airflow3_api_collection_updated.json")
    result_path = "/usr/local/airflow/include/result.txt"
    bearer_token = ""
    if "cloud.astronomer-stage.io" in os.environ["AIRFLOW__ASTRONOMER__CLOUD_UI_URL"]:
        bearer_token = os.environ["STAGE_ORG_TOKEN"]
    elif "cloud.astronomer-dev.io" in os.environ["AIRFLOW__ASTRONOMER__CLOUD_UI_URL"]:
        bearer_token = os.environ["DEV_ORG_OWNER"]

    @task
    def run_api_test():
        # Open the JSON file for reading
        with open(postman_collection) as file:
            data = json.load(file)

        """
        This code is commented as I have tested it locally on breeze and auth and url change is not required
        Later this code will be required when we need to test this on different env's
        """
        # Make the updates
        # data['values'][0]['value'] = f"{os.environ['AIRFLOW__WEBSERVER__BASE_URL']}/api/v1/"

        # data['variable'][1]['value'] = bearer_token

        # Save the modified data back to the file
        with open(postman_collection_updated, "w") as file:
            json.dump(data, file, indent=4)

        print("JSON file has been updated.")

        data = subprocess.run(["newman", "run", postman_collection_updated], capture_output=True, text=True)
        print(data.stdout)
        return data.stdout

    run_api_test()
