import textwrap

import pendulum
import psycopg2
from airflow import settings
from airflow.models.xcom import BaseXCom
from airflow.utils.session import provide_session
from sqlalchemy.orm.session import Session


class DB:
    # use the airflow db
    def get_connection():
        url = settings.engine.url
        host = url.host or ""
        port = str(url.port or "5432")
        user = url.username or ""
        password = url.password or ""
        schema = url.database
        conn = psycopg2.connect(host=host, user=user, port=port, password=password, dbname=schema)
        return conn

    # print queries as we run them,
    # and notices as the appear,
    # and commit after each call
    @staticmethod
    def runsql(conn, cursor, sql):
        print("query:")
        cursor.execute(sql)
        print(textwrap.indent(sql, "    "))
        try:
            for notice in conn.notices:
                print(notice)
            del conn.notices[:]

        except AttributeError:
            pass
        conn.commit()


class SQL:
    create = textwrap.dedent(
        """
        CREATE TABLE IF NOT EXISTS ycom (
            dag_id VARCHAR(255)
            , task_id VARCHAR(255)
            , execution_date_or_run_id VARCHAR(255)
            , key VARCHAR(255)
            , value VARCHAR(255) NOT NULL
        );
        """
    )

    @staticmethod
    def set(ref, value):
        return textwrap.dedent(
            f"""
            INSERT INTO ycom
            VALUES ('{ref["dag_id"]}'
                    , '{ref["task_id"]}'
                    , '{ref["execution_date_or_run_id"]}'
                    , '{ref["key"]}'
                    , '{value}'
                    );
            """
        )

    @staticmethod
    def get(ref):
        return textwrap.dedent(
            f"""
            SELECT value
            FROM ycom
            WHERE dag_id = '{ref["dag_id"]}'
                AND task_id = '{ref["task_id"]}'
                AND execution_date_or_run_id = '{ref["execution_date_or_run_id"]}'
                AND key = '{ref["key"]}'
            ;
            """
        )

    @staticmethod
    def clear(dag_id, task_id, execution_date_or_run_id):
        return textwrap.dedent(
            f"""
                DELETE
                FROM ycom
                WHERE dag_id = '{dag_id}'
                    AND task_id = '{task_id}'
                    AND execution_date_or_run_id = '{execution_date_or_run_id}'
                ;
                """
        )

    delete = "DROP TABLE ycom;"


class YCom(BaseXCom):
    """
    YCom is a custom implementation of XCom.
    It's worse than XCom in every way, don't use it,
    unless you're testing custom XCom Backends.
    """

    @classmethod
    @provide_session
    def set(cls, key, value, task_id, dag_id, run_id, session=None):
        # build a reference for ycom to use
        reference = {
            "key": key,
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date_or_run_id": str(run_id),
        }

        # pass reference and value together
        super().set(key, (reference, value), task_id, dag_id, run_id=run_id, session=session)

    @staticmethod
    def serialize_value(
        cls,
        key,
        value,
        task_id,
        dag_id,
        run_id,
    ):
        print("serialize_value got:", value)
        reference, value = value

        # store the value in a custom location
        connection = DB.get_connection()
        cursor = connection.cursor()
        DB.runsql(connection, cursor, SQL.create)
        DB.runsql(connection, cursor, SQL.set(reference, value))

        # serialize and store the reference in traditional XCOM
        ref_str = BaseXCom.serialize_value(reference)
        return ref_str

    @staticmethod
    def deserialize_value(serialized_ref):
        # deserialize the reference from traditional xcom
        reference = BaseXCom.deserialize_value(serialized_ref)

        # retrieve the value from the custom location
        connection = DB.get_connection()
        cursor = connection.cursor()
        DB.runsql(connection, cursor, SQL.get(reference))
        value = cursor.fetchone()[0]

        return value

    @staticmethod
    def clear(
        execution_date: pendulum.DateTime = None,
        dag_id: str = None,
        task_id: str = None,
        run_id: str = None,
        session: Session = None,
    ):
        # runs before each task to make way for new XCOMs

        # clear the custom backend for this task instance only
        connection = DB.get_connection()
        cursor = connection.cursor()

        DB.runsql(connection, cursor, SQL.create)
        DB.runsql(connection, cursor, SQL.clear(dag_id, task_id, execution_date or run_id))
