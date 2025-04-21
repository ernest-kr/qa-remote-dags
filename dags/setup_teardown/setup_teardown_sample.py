import json
import os

import pendulum
from airflow.decorators import dag, setup, task, teardown
from airflow.sdk import DAG

with DAG(
    dag_id="setup_and_teardown_sample",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["setup_teardown"],
) as dag:

    @setup
    @task
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        with open("test_file.txt", "w+") as file:
            file.write(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task
    def load(total_order_value: float):
        print(f"Total order value is: {total_order_value:.2f}")

    @teardown()
    @task
    def delete_file():
        if os.path.exists("test_file.txt"):
            os.remove("test_file.txt")

    order_data = extract()
    transform = transform(order_data)
    delete_file = delete_file()

    with order_data >> delete_file:
        transform >> load(transform["total_order_value"])
