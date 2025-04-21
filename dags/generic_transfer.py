from datetime import datetime
from textwrap import indent

from airflow.decorators import task
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG
from elephantsql_kashin import conn_id as postgres_conn_id
from sqlite_pocketsand import conn_id as sqlite_conn_id, dockerfile_fragment, table_name

doc = f"""
Tests the generic transfer operator, which reads a table from one database and places it in another.

Requires a postgres connection ({postgres_conn_id}) and a sqlite connection ({postgres_conn_id})
Run the dags with those names first to create them

Also requires a sqlite db (probably built into the image like so):
{indent(dockerfile_fragment, "    ")}
"""

DROP = f"DROP TABLE IF EXISTS {table_name}"
CREATE = f"CREATE TABLE IF NOT EXISTS {table_name}(name varchar, age integer)"
INSERT = f"INSERT INTO {table_name}(name, age) VALUES ('Chi', 23), ('Fred', 25)"
SELECT = f"SELECT * FROM {table_name}"

with DAG(
    "generic_transfer_dag",
    schedule=None,
    start_date=datetime(1970, 1, 1),
    tags=["core_neg"],
) as dag:
    transfer = GenericTransfer(
        task_id="transfer_to_psql",
        sql=SELECT,
        preoperator=[DROP, CREATE],
        source_conn_id=sqlite_conn_id,
        destination_conn_id=postgres_conn_id,
        destination_table=table_name,
    )

    drop = SQLExecuteQueryOperator(
        task_id="drop",
        conn_id=postgres_conn_id,
        sql=DROP,
    )

    @task
    def check():
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        result = pg_hook.get_records(sql=f"SELECT age FROM {table_name} WHERE name='Fred'")
        print(f"got {result}")
        assert result[0][0] == 25

    drop >> transfer >> check()
