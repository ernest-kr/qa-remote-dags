from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration
import logging


# Define a callback function that logs error details when a task fails.
def task_failure_alert(context):
    task_instance = context.get("task_instance")
    error = context.get("exception")
    # Log error details. You can also send alerts or emails from here.
    logging.error("Task %s failed. Error: %s", task_instance.task_id, error)


# Connection and Snowflake details.
_SNOWFLAKE_CONN_ID = "SNOWFLAKE_DEFAULT"
SQL_QUERY = "insert into SREENU_TABLE (num, name, age, contact) values (14,'sreenu13',34,'1234');"


@dag(
    dag_id="sreenu_snowflake_connection_sreenu_db",
    start_date=datetime(2024, 9, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": duration(seconds=5),
        # You can specify the failure callback in default_args so all tasks use it.
        "on_failure_callback": task_failure_alert,
    },
    doc_md="""
    ### Test Snowflake Connection DAG

    This DAG tests the Snowflake connection by executing a simple query.
    If a task fails (for example, due to a connection error), a custom failure callback logs the error.
    """,
    tags=["snowflake"],
)
def test_snowflake_connection():
    # Run a simple SQL query to get the current date from Snowflake.
    test_query = SQLExecuteQueryOperator(
        task_id="test_query",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=SQL_QUERY,
        on_failure_callback=task_failure_alert,
    )

    test_query


# Instantiate the DAG.
test_snowflake_connection()
