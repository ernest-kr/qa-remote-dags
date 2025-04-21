import json
import os

import pendulum
from airflow.decorators import dag, setup, task, task_group, teardown


@dag(schedule=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["setup_teardown"])
def task_group_example():
    @setup
    @task
    def extract_data():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        with open("test_file.txt", "w+") as file:
            file.write(data_string)
        return order_data_dict

    @task_group
    def transform_values(order_data_dict):
        @setup
        @task
        def process_data(order_data_dict):
            order_data_dict_init = {key: int(value) for key, value in order_data_dict.items()}
            with open("test_file_tg.txt", "w+") as file:
                file.write(str(order_data_dict_init))
            return order_data_dict_init

        @task
        def transform_sum(order_data_dict: dict):
            total_order_value = 0
            for value in order_data_dict.values():
                total_order_value += value

            return {"total_order_value": total_order_value}

        @task
        def transform_avg(order_data_dict: dict):
            total_order_value = 0
            for value in order_data_dict.values():
                total_order_value += value
                avg_order_value = total_order_value / len(order_data_dict)

            return {"avg_order_value": avg_order_value}

        @teardown
        @task
        def delete_file():
            if os.path.exists("test_file_tg.txt"):
                os.remove("test_file_tg.txt")

        process_data = process_data(order_data_dict)
        delete = delete_file()

        with process_data >> delete:
            avg = transform_avg(process_data)
            sum = transform_sum(process_data)
            avg >> sum

        return {
            "avg": avg,
            "total": sum,
        }

    @task
    def load(order_values: dict):
        print(
            f"""Total order value is: {order_values['total']['total_order_value']:.2f}
            and average order value is: {order_values['avg']['avg_order_value']:.2f}"""
        )

    @teardown
    @task
    def delete_file():
        if os.path.exists("test_file.txt"):
            os.remove("test_file.txt")

    s = extract_data()
    t = delete_file()
    with s >> t:
        load(transform_values(s))


task_group_example()
