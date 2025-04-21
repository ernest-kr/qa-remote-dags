"""
### Create a custom sensor using the @task.sensor decorator

This DAG showcases how to create a custom sensor using the @task.sensor decorator
to check the availability of an API.
"""

import requests
from airflow.decorators import dag, setup, task, teardown

# importing the PokeReturnValue
from airflow.sensors.base import PokeReturnValue
from pendulum import datetime


@dag(start_date=datetime(2022, 12, 1), schedule=None, catchup=False)
def sensor_decorator_setup_teardown():
    # supply inputs to the BaseSensorOperator parameters in the decorator

    @setup
    @task.sensor(poke_interval=1, timeout=10, mode="poke")
    def check_shibe_availability() -> PokeReturnValue:
        r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
        print(r.status_code)

        # set the condition to True if the API response was 200
        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.json()
        else:
            condition_met = False
            operator_return_value = None
            print(f"Shibe URL returned the status code {r.status_code}")

        # the function has to return a PokeReturnValue
        # if is_done = True the sensor will exit successfully, if
        # is_done=False, the sensor will either poke or be rescheduled
        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    # print the URL to the picture
    @task
    def print_shibe_picture_url(url):
        print(url)

    @teardown
    @task
    def teardown_task():
        print("teardown")

    task_1 = check_shibe_availability()
    task_2 = print_shibe_picture_url(task_1)
    task_3 = teardown_task()
    task_1 >> task_2 >> task_3.as_teardown(setups=task_1)


sensor_decorator_setup_teardown()
