from datetime import datetime, timedelta

from airflow import Dataset
from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync

# these two will have downstream dags
sensor = Dataset("sensor")
asyncSensor = Dataset("asyncSensor")

# these two will not
sensor2 = Dataset("sensor2")
asyncSensor2 = Dataset("asyncSensor2")


@dag(start_date=datetime(1970, 1, 1), schedule=None, tags=["datasets"])
def dataset_sensors():
    TimeDeltaSensor(task_id="wait5", outlets=[sensor], delta=timedelta(seconds=5))
    TimeDeltaSensorAsync(task_id="wait5async", outlets=[asyncSensor], delta=timedelta(seconds=5))
    TimeDeltaSensor(task_id="wait5_2", outlets=[sensor2], delta=timedelta(seconds=5))
    TimeDeltaSensorAsync(task_id="wait5async_s", outlets=[asyncSensor2], delta=timedelta(seconds=5))


dataset_sensors()


@dag(start_date=datetime(1970, 1, 1), schedule=[sensor], is_paused_upon_creation=False)
def dataset_sensor_sink():
    EmptyOperator(task_id="dummy")


dataset_sensor_sink()


@dag(start_date=datetime(1970, 1, 1), schedule=[asyncSensor], is_paused_upon_creation=False)
def async_dataset_sensor_sink():
    EmptyOperator(task_id="dummy")


async_dataset_sensor_sink()
