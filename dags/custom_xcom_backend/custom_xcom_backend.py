import textwrap

import psycopg2
from airflow import settings
from airflow.decorators import dag, task
from pendulum import today

disclaimer = textwrap.dedent(
    """
    The following task will only succeed if the provided custom XCOM is enabled,
    to do this, add the following line to your Dockerfile:
        ENV AIRFLOW__CORE__XCOM_BACKEND=ycom.YCom

    Also make sure that the plugins folder contains ycom.py
    """
)


class DB:
    @staticmethod
    def runsql(conn, cursor, sql):
        "for transparently executing queries"

        print("query:")
        cursor.execute(sql)
        print(textwrap.indent(sql, "    "))
        try:
            for notice in conn.notices:
                print(notice)
            del conn.notices[:]

        except AttributeError:
            pass


payload = textwrap.dedent(
    """
    Awkward grammar appals a craftsman. A Dada bard
    as daft as Tzara damns stagnant art and scrawls an
    alpha (a slapdash arc and a backward zag) that mars
    all stanzas and jams all ballads (what a scandal)
    """
)


class SQL:
    get_all = "SELECT * FROM ycom;"

    count_payload_rows = "SELECT COUNT(*) FROM ycom WHERE task_id = 'place_xcom';"

    get_exists = textwrap.dedent(
        """
        SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE tablename  = 'ycom'
        );
        """
    )


def get_connection():
    url = settings.engine.url
    host = url.host or ""
    port = str(url.port or "5432")
    user = url.username or ""
    password = url.password or ""
    schema = url.database
    conn = psycopg2.connect(host=host, user=user, port=port, password=password, dbname=schema)
    return conn


def get_row_count_task(tag):
    @task(task_id=f"count_payload_rows_{tag}")
    def count_rows():
        conn = get_connection()
        cursor = conn.cursor()
        DB.runsql(conn, cursor, SQL.count_payload_rows)
        count = cursor.fetchone()[0]
        print("row count", count)
        return count

    return count_rows


@task
def place_xcom():
    return payload


@task
def read_xcom(value):
    print(value)
    assert value == payload


@task
def ensure_payload():
    conn = get_connection()
    cursor = conn.cursor()
    DB.runsql(conn, cursor, SQL.get_all)

    # get all results as a single string
    onestr = ""
    for row in cursor.fetchall():
        for column in row:
            print(column)
            onestr += column

    # our payload should be in it
    assert payload in onestr


@task
def compare_counts(before, after):
    print(f"payload rows before {before}")
    print(f"payload rows after {after}")

    assert int(after) == int(before) + 1


@dag(
    schedule=None,
    start_date=today("UTC").add(days=-10),
    default_args={"owner": "airflow"},
    max_active_runs=1,
    catchup=False,
    doc_md=disclaimer,
    tags=["xcom_backend"],
)
def custom_xcom_backend():
    payload = place_xcom()
    before = get_row_count_task("before")()
    after = get_row_count_task("after")()

    (before >> payload >> read_xcom(payload) >> ensure_payload() >> after >> compare_counts(before, after))


the_dag = custom_xcom_backend()
