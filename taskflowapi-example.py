import requests
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 03, 26)
}

API = "https://api.openweathermap.org/data/2.5/weather?q=California&appid=44486e83f757f27e90e2b8beab2fe34a"


@dag(default_args=default_args, schedule_interval=None, tags=['core'])
def taskflow_api_etl():
    @task()
    def extract():
        response = requests.get(API).json()
        response = {"main": response["main"], "city": response["name"]}
        return response

    @task(multiple_outputs=True)
    def transform(response: dict):

        data = {
            "Maximum Temperature": str(response["main"]["temp_max"] - 273.15)
            + " Celsius",
            "Minimum Temperature": str(response["main"]["temp_min"] - 273.15)
            + " Celcius",
            "City": response["city"],
        }
        return data

    @task()
    def load(data: dict):
        print("Weather info: ", data)

    load(transform(extract()))


etl_dag = taskflow_api_etl()
