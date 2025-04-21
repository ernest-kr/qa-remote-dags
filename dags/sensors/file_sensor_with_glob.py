from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.state import TaskInstanceState
from plugins.airflow_dag_introspection import add_conn, assert_the_task_states, delete_conn, log_checker

docs = """
####Purpose
This dag recursively searches a directory for log files.\n
This dags purpose is to test the FileSensors new feature recursively searching a filepath with the ** glob character.\n
This dag achieves this test by testing for the presence of any log file in /usr/local/airflow/logs/scheduler/latest .\n
Because of the fact that it needs to test for a log file this dag cannot be the first in either the batch or turbulence block.\n
####Expected Behavior
This dag has 5 tasks all of which are expected to succeed.\n
The 1st task sets up a filepath connection for testing.\n
The 2nd task senses for any file in the filepath set in the connection in the 1st task with the FileSensor\n
The 3rd task deletes the connection created in the 1st task.\n
The 4th task checks that the sensor returned "Success criteria met. Exiting." in the logs of the FileSensor in the 2nd task ensuring that the Sensor found the file correctly.\n
The 5th task checks that all of the preceding tasks are successful and that itself is in the running state.
"""

with DAG(
    dag_id="filesystem_sensor_glob",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    max_active_tasks=1,
    max_active_runs=1,
    doc_md=docs,
    tags=["file_system_sensor", "sensor"],
) as dag:
    conn_id = "FileSensorGlob"

    t0 = PythonOperator(
        task_id="add_conn_introspected",
        python_callable=add_conn,
        op_args=[
            conn_id,
            "File (path)",
            "/opt",
            None,
            None,
            None,
            None,
        ],
    )

    t1 = FileSensor(
        task_id="sense_all_files",
        fs_conn_id=conn_id,
        filepath="/usr/local/airflow/dags/*.py*",
        recursive=True,
        timeout=220,
        poke_interval=20,
    )

    t2 = PythonOperator(
        task_id="del_conn",
        python_callable=delete_conn,
        op_args=[conn_id],
    )

    t3 = (
        PythonOperator(
            task_id="check_logs",
            retries=5,
            python_callable=log_checker,
            op_args=[
                "sense_all_files",
                "Success criteria met. Exiting.",
                "Task exited with return code 1",
            ],
        ),
    )

    t4 = PythonOperator(
        task_id="assert_task_states",
        python_callable=assert_the_task_states,
        op_args=[
            {
                "add_conn_introspected": TaskInstanceState.SUCCESS,
                "sense_all_files": TaskInstanceState.SUCCESS,
                "del_conn": TaskInstanceState.SUCCESS,
                "check_logs": TaskInstanceState.SUCCESS,
                "assert_task_states": TaskInstanceState.RUNNING,
            }
        ],
    )

t0 >> t1 >> t2 >> t3 >> t4
