from datetime import date, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.elephantsql_kashin import conn_id as postgres_conn_id

dag_name = "example_soft_fail_sensor_param"

DATES = []
for i in range(6):
    DATES.append((date.today() - timedelta(days=i)).strftime("%Y-%m-%d"))

TABLE = "checkt"
DROP = f"DROP TABLE IF EXISTS {TABLE} CASCADE;"
CREATE = f"CREATE TABLE IF NOT EXISTS {TABLE}(state varchar, temp integer, date date)"
INSERT = f"""
    INSERT INTO {TABLE}(state, temp, date)
    VALUES ('Lagos', 23, '{DATES[4]}'),
        ('Enugu', 25, '{DATES[3]}'),
        ('Delta', 25, '{DATES[2]}'),
        ('California', 28, '{DATES[1]}'),
        ('Abuja', 25, '{DATES[0]}')
    """


def prepare_data():
    postgres = PostgresHook(postgres_conn_id)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DROP)
            cur.execute(CREATE)
            cur.execute(INSERT)
        conn.commit()


def temp(name):
    return name == "Abia"


with DAG(
    dag_id=dag_name,
    default_args={"owner": "airflow", "start_date": today("UTC").add(days=-2)},
    schedule=None,
    tags=["psql", "sensor"],
) as dag:
    t1 = PythonOperator(task_id="prepare_table", python_callable=prepare_data)

    t2 = BashOperator(task_id="sleep_60", bash_command="sleep 45")

    t3 = SqlSensor(
        task_id="sql_sensor_skip_on_failure",
        conn_id=postgres_conn_id,
        sql=f"SELECT * FROM {TABLE} WHERE state='Abia'",
        parameters=["state", "temp", "date"],
        # calls the function 'temp' which returns "Abia"
        success=temp,
        soft_fail=True,
        poke_interval=11,
        timeout=33,
    )

    t4 = SQLExecuteQueryOperator(
        conn_id=postgres_conn_id,
        task_id="drop_table_last",
        sql=DROP,
    )
    t1 >> t2 >> t4
    t1 >> t3
