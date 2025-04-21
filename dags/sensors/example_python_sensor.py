from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG
from pendulum import today


def check_value(**kwargs):
    return kwargs.get("total", 1) == 2


def check_args(*args):
    for i in args:
        if i == 2:
            return True
    return False


def check_temp(**kwargs):
    return kwargs["templates_dict"]["value"] == 2


with DAG(
    "example_python_sensor",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["sensor"],
) as dag:
    t1 = PythonSensor(task_id="check_value", python_callable=check_value, op_kwargs={"total": 2})
    t2 = PythonSensor(task_id="check_args", python_callable=check_args, op_args=[1, 3, 4, 2])
    t3 = PythonSensor(task_id="temp_dict", python_callable=check_temp, templates_dict={"value": 2})

    t1 >> t2 >> t3
