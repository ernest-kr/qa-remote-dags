import textwrap
from datetime import datetime, timedelta

import psycopg2
from airflow import Dataset, settings
from airflow.decorators import dag, task

# when simple_dataset_source updates this dataset
# simple_dataset_sink will run
counter = Dataset("counter")


# we're using this table as a dataset
# airflow doesn't know about it, it just looks at the above object
update_template = textwrap.dedent(
    """
    BEGIN;

    -- make sure the table exists
    CREATE TABLE IF NOT EXISTS counter (
        source INT NOT NULL,
        sink INT NOT NULL
    );

    -- seed it if it's newly created
    INSERT INTO counter (source, sink)
    SELECT 0, 0
    WHERE NOT EXISTS (SELECT * FROM counter);

    -- increment based on coaller
    UPDATE counter
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


@task(outlets=[counter])
def increment_source():
    cursor = get_cursor()
    cursor.execute(update_template.format(column="source"))
    cursor.execute("SELECT * FROM counter;")
    print(cursor.fetchall())


@task
def increment_sink():
    cursor = get_cursor()
    cursor.execute(update_template.format(column="sink"))
    cursor.execute("SELECT * FROM counter;")
    print(cursor.fetchall())


@task
def reset():
    cursor = get_cursor()
    cursor.execute("DROP TABLE IF EXISTS counter;")


# run this before the test
@dag(start_date=datetime(1970, 1, 1), schedule=None, tags=["datasets"])
def simple_dataset_reset():
    reset()


# unpause this
@dag(start_date=datetime(1970, 1, 1), schedule=timedelta(days=365 * 30), tags=["datasets"])
def simple_dataset_source():
    increment_source()


# expect this to run
@dag(schedule=[counter], start_date=datetime(1970, 1, 1), is_paused_upon_creation=False)
def simple_dataset_sink():
    increment_sink()
    # task logs should say 1,1
    # source ran once
    # sink ran once


reset_dag = simple_dataset_reset()
source = simple_dataset_source()
sink = simple_dataset_sink()
