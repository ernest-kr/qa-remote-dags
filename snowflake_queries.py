from datetime import datetime, timedelta
import random
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_SNOWFLAKE_CONN_ID = "vandyliu_feb17"
_SNOWFLAKE_NAMESPACE = "SNOWFLAKE://GP21411.US-EAST-1.AWS"
_SNOWFLAKE_INLET_DATASET = "SANDBOX.VANDYLIU.CUSTOMERS"
_SNOWFLAKE_OUTLET_DATASET = "SANDBOX.VANDYLIU.ORDERS"

customer_dataset = Dataset(
    name=_SNOWFLAKE_INLET_DATASET,
    namespace=_SNOWFLAKE_NAMESPACE,
    facets={}
)

orders_dataset = Dataset(
    name=_SNOWFLAKE_OUTLET_DATASET,
    namespace=_SNOWFLAKE_NAMESPACE,
    facets={}
)

def task_failure_alert(context):
    task_instance = context.get("task_instance")
    error = context.get("exception")
    # Log error details. You can also send alerts or emails from here.
    logging.error("Task %s failed. Error: %s", task_instance.task_id, error)

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def task_1_logic():
    print(f"Starting to run task 1...")
    current_day = datetime.today().strftime('%A')
    if current_day == "Monday":
        # This will return a snowflake SQL error to simulate a real-world scenario
        return "USE ROLE ACCOUNTADMIN;"
    print("Task 1 completed successfully.")
    return "SELECT * FROM customers;"

# Define DAG
dag = DAG(
    'sreenu_snowflake_test',
    default_args=default_args,
    description='A DAG with complex task dependencies and logic.',
    schedule_interval='30 19 * * *',
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=['example', 'snowflake'],
)

# Define tasks
task_1 = SQLExecuteQueryOperator(
    task_id="extract_data",
    conn_id=_SNOWFLAKE_CONN_ID,
    sql=task_1_logic(),
    on_failure_callback=task_failure_alert,
    inlets=[customer_dataset],
    dag=dag
)


# Set dependencies
task_1