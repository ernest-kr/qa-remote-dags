from datetime import date, timedelta

from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLCheckOperator,
    SQLExecuteQueryOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from pendulum import today
from plugins.airflow_dag_introspection import assert_the_task_states
from plugins.elephantsql_kashin import conn_id as CONN_ID

docs = """
####Info
This dag requires a postgres connection to be configured before you run it,\n
run dag_id: 'elephantsql_kashin' to create that connection.
####Purpose
This dag tests BranchSQLOperator, SQLCheckOperator, SQLIntervalCheckOperator, SQLThresholdCheckOperator and SQLValueCheckOperator
####Expected Behavior
This dag has 11 tasks 9 of which are expected to succeed and 2 tasks that are expected to be skipped.\n
This dag should pass.

"""

DATES = []
for i in range(6):
    DATES.append((date.today() - timedelta(days=i)).strftime("%Y-%m-%d"))

TABLE = "checktable"
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

SQLBOOL_QUERY = f"""
SELECT CAST(CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS BIT)
FROM {TABLE} WHERE temp = 30;
"""


def prepare_data():
    postgres = PostgresHook(CONN_ID)
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DROP)
            cur.execute(CREATE)
            cur.execute(INSERT)
        conn.commit()


with DAG(
    dag_id="sql_check_operator",
    default_args={"owner": "airflow", "start_date": today("UTC").add(days=-2)},
    schedule=None,
    tags=["core", "psql"],
    doc_md=docs,
) as dag:
    t1 = PythonOperator(task_id="prepare_table", python_callable=prepare_data)

    t2 = SQLCheckOperator(task_id="sql_check", sql=f"SELECT COUNT(*) FROM {TABLE}", conn_id=CONN_ID)
    t3 = SQLValueCheckOperator(
        task_id="sql_check_value",
        sql=f"SELECT COUNT(*) FROM {TABLE}",
        pass_value=5,
        conn_id=CONN_ID,
    )
    t4 = BranchSQLOperator(
        conn_id=CONN_ID,
        task_id="branch_sql",
        sql=SQLBOOL_QUERY,
        follow_task_ids_if_false="add_state",
        follow_task_ids_if_true="remove_state",
    )
    t5 = SQLExecuteQueryOperator(
        conn_id=CONN_ID,
        task_id="remove_state",
        sql=f"DELETE FROM {TABLE} WHERE name='Bob'",
    )

    d0 = EmptyOperator(task_id="dummy0")

    t6 = SQLExecuteQueryOperator(
        conn_id=CONN_ID,
        task_id="add_state",
        sql=f"INSERT INTO {TABLE} (state, temp, date) VALUES ('Abia', 25, '{DATES[5]}')",
    )
    t7 = SQLThresholdCheckOperator(
        conn_id=CONN_ID,
        task_id="check_threshold",
        min_threshold=5,
        max_threshold=7,
        sql=f"SELECT COUNT(*) FROM {TABLE}",
    )
    t8 = SQLIntervalCheckOperator(
        conn_id=CONN_ID,
        task_id="interval_check",
        table=TABLE,
        days_back=3,
        date_filter_column="date",
        metrics_thresholds={"temp": 24},
    )

    t9 = SQLExecuteQueryOperator(
        conn_id=CONN_ID,
        task_id="drop_table_last",
        sql=f"DROP TABLE IF EXISTS {TABLE} CASCADE;",
    )

    t10 = PythonOperator(
        task_id="check_task_states",
        python_callable=assert_the_task_states,
        op_kwargs={
            "task_ids_and_assertions": {
                "prepare_table": TaskInstanceState.SUCCESS,
                "sql_check": TaskInstanceState.SUCCESS,
                "sql_check_value": TaskInstanceState.SUCCESS,
                "branch_sql": TaskInstanceState.SUCCESS,
                "add_state": TaskInstanceState.SUCCESS,
                "check_threshold": TaskInstanceState.SUCCESS,
                "interval_check": TaskInstanceState.SUCCESS,
                "drop_table_last": TaskInstanceState.SUCCESS,
                "remove_state": TaskInstanceState.SKIPPED,
                "dummy0": TaskInstanceState.SKIPPED,
            }
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1 >> t2 >> t3 >> t4 >> [t6, t5]
    t6 >> t7 >> t8 >> t9 >> t10
    t5 >> d0
