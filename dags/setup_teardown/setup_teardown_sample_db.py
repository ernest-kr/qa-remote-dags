import textwrap
from datetime import datetime

import psycopg2
from airflow import settings
from airflow.decorators import dag, setup, task, teardown

update_template = textwrap.dedent(
    """
    BEGIN;

    -- make sure the table exists
    CREATE TABLE IF NOT EXISTS test_table (
        value INT NOT NULL
    );

    -- seed it if it's newly created
    INSERT INTO test_table (value)
    SELECT 0
    WHERE NOT EXISTS (SELECT * FROM test_table);

    -- increment based on coaller
    UPDATE test_table
    SET {column} = {column} + 1;

    COMMIT;
    """
)


def get_cursor():
    "get a cursor on the airflow database"
    url = settings.engine.url
    host = url.host or "localhost"
    port = str(url.port or "5432")
    user = url.username or "postgres"
    password = url.password or "postgres"
    schema = url.database

    conn = psycopg2.connect(host=host, user=user, port=port, password=password, dbname=schema)
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor


@setup
@task
def create_table():
    cursor = get_cursor()
    cursor.execute(update_template.format(column="value"))
    cursor.execute("SELECT * FROM test_table;")
    value = cursor.fetchall()[0][0]
    print(f"value is {value}")
    return value


@task()
def transform(value: int):
    value = value + 1
    return value


@task
def load(value: int):
    print(f"Total value is: {value}")


@teardown
@task
def delete_table():
    cursor = get_cursor()
    cursor.execute("DROP TABLE test_table")


@dag(start_date=datetime(1970, 1, 1), schedule=None, tags=["setup_teardown"])
def setup_teardown_sample_db():
    s = create_table()
    t = delete_table()

    with s >> t:
        load(transform(s))


setup_teardown_sample = setup_teardown_sample_db()
